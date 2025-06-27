import os

from Reader import LogReader
from Config import Config

from Connector import Connector
from Reader import LogReader
from Reader import LogAnalyzer



if __name__ == '__main__':

    config = Config("config.yaml")

    file_path = config.get_reader_dir_path()
    server_address = config.get_server_address()

    # Connect to remote server
    connector = Connector(server_address)
    spark = connector.connect().get_session()


    print("session : ", spark.version)

    # Read raw logs from log files
    reader = LogReader(spark, file_path)
    df = reader.read_file()
    #
    # # Analyzing step
    #
    #
    # #

    analyzer = LogAnalyzer()

    fdf = analyzer.filter_by_status(df, "404")
    fdf = fdf.groupby("status").count()
    fdf.show()
    # ip_count = df.groupby("status").count()
    # ip_count.show()

