import os

from Reader import LogReader
from Config import Config



if __name__ == '__main__':

    config = Config("config.yaml")

    file_path = config.get_reader_dir_path()


    log_reader = LogReader(file_path)


    for batch in log_reader.read_file(10):
        pass