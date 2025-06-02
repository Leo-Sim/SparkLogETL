import os

from Reader import LogReader
from Config import Config

from Connector import Connector




if __name__ == '__main__':

    config = Config("config.yaml")

    file_path = config.get_reader_dir_path()
    server_address = config.get_server_address()

    connector = Connector(server_address)

    session = connector.connect().get_session()

    print("spark : ", session)
    session.sql("SELECT 'hello' AS msg").show()

    session.sql("SELECT 'hello2222' AS msg").show()
    # spark =

    # log_reader = LogReader(file_path)
    #
    #
    # for batch in log_reader.read_file(10):
    #     pass