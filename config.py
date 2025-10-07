# config.py
from configparser import ConfigParser
import os

def load_config(filename='config.ini', section='postgresql'):
    """ Load database configuration from a file. """
    # Get the path to the directory where the script is located
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to postgresql
    config = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            config[param[0]] = param[1]
    else:
        raise Exception(f'Section {section} not found in the {filename} file')

    return config