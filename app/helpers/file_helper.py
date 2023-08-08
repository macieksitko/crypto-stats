import os


def is_file_empty(file):
    return os.stat(file).st_size == 0