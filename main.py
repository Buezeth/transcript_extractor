# main.py
import sys
import concurrent.futures
from database import get_connection, fetch_videos_to_process, save_transcript_and_update_status, mark_video_as_failed
from scraper import process_video

def main(chunk_size):
    print(f"Starting transcript processing with a chunk size of {chunk_size}.")
    
    conn = get_connection()
    if not conn:
        print("Could not connect to the database. Exiting.")
        return

    try:
        while True:
            # 1. Fetch a chunk of videos to process
            videos = fetch_videos_to_process(conn, chunk_size)
            if not videos:
                print("No more 'pending' videos found. All work is done.")
                break

            print(f"Fetched a batch of {len(videos)} videos to process.")
            
            # 2. Process the chunk in parallel using a ThreadPool
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                # The executor maps the process_video function to each video tuple
                future_to_video = {executor.submit(process_video, video_db_id, video_id): video_db_id for video_db_id, video_id in videos}
                
                # 3. As results complete, save them to the database
                for future in concurrent.futures.as_completed(future_to_video):
                    video_db_id = future_to_video[future]
                    try:
                        # result is a tuple: (video_db_id, transcript_chunks)
                        completed_video_id, transcript_chunks = future.result()
                        
                        if transcript_chunks:
                            # The video was processed successfully
                            video_id_from_db = [v[1] for v in videos if v[0] == completed_video_id][0]
                            save_transcript_and_update_status(conn, completed_video_id, video_id_from_db, transcript_chunks)
                        else:
                            # The process failed (e.g., no transcript found)
                            mark_video_as_failed(conn, completed_video_id)
                            
                    except Exception as exc:
                        print(f'Video (DB ID: {video_db_id}) generated an exception: {exc}')
                        mark_video_as_failed(conn, video_db_id)

    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python main.py <chunk_size>")
        sys.exit(1)
    
    try:
        chunk_size_arg = int(sys.argv[1])
        if chunk_size_arg <= 0:
            raise ValueError
    except ValueError:
        print("Error: chunk_size must be a positive integer.")
        sys.exit(1)
        
    main(chunk_size_arg)