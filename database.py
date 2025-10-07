# database.py
import psycopg2
from config import load_config

def get_connection():
    """ Establish and return a database connection. """
    try:
        config = load_config()
        conn = psycopg2.connect(**config)
        return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Error connecting to the database: {error}")
        return None

def fetch_videos_to_process(conn, chunk_size):
    """
    Atomically fetches a batch of 'pending' videos and marks them as 'processing'.
    This is safe for concurrent workers.
    """
    videos_to_process = []
    sql = """
        UPDATE videos
        SET status = 'processing', updated_at = CURRENT_TIMESTAMP
        WHERE id IN (
            SELECT id
            FROM videos
            WHERE status = 'pending'
            ORDER BY created_at
            LIMIT %s
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, video_id;
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (chunk_size,))
            videos_to_process = cur.fetchall()
            conn.commit()
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Error fetching videos: {error}")
        conn.rollback()
    
    return videos_to_process

def save_transcript_and_update_status(conn, video_db_id, video_id, transcript_chunks):
    """
    Saves transcript chunks to the database and marks the video as 'completed'.
    """
    insert_sql = "INSERT INTO transcripts (video_table_id, video_id, transcript_chunk, chunk_order) VALUES (%s, %s, %s, %s);"
    update_sql = "UPDATE videos SET status = 'completed', updated_at = CURRENT_TIMESTAMP WHERE id = %s;"
    
    try:
        with conn.cursor() as cur:
            for i, chunk in enumerate(transcript_chunks):
                cur.execute(insert_sql, (video_db_id, video_id, chunk, i + 1))
            
            cur.execute(update_sql, (video_db_id,))
            conn.commit()
            print(f"Successfully saved transcript for video_id: {video_id}")
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Error saving transcript for video_id {video_id}: {error}")
        conn.rollback()

def mark_video_as_failed(conn, video_db_id):
    """ Marks a video as 'failed' in the database. """
    sql = "UPDATE videos SET status = 'failed', updated_at = CURRENT_TIMESTAMP WHERE id = %s;"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (video_db_id,))
            conn.commit()
            print(f"Marked video with db_id {video_db_id} as failed.")
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Error marking video as failed for db_id {video_db_id}: {error}")
        conn.rollback()