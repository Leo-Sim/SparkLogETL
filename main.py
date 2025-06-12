import os

from Reader import LogReader
from Config import Config

from Connector import Connector
from Reader import LogReader



if __name__ == '__main__':

    config = Config("config.yaml")

    file_path = config.get_reader_dir_path()
    server_address = config.get_server_address()

    connector = Connector(server_address)

    spark = connector.connect().get_session()

    reader = LogReader(spark, file_path)

    reader.read_file()
