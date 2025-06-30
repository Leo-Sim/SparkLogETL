import os

from Reader import LogReader, LogField
from Config import Config

from Connector import Connector
from Reader import LogReader
from Reader import LogAnalyzer



if __name__ == '__main__':

    config = Config("config.yaml")

    # -------- configuration information -----------
    file_path = config.get_reader_dir_path()
    server_address = config.get_server_address()

    brute_login_count_th = config.get_brute_force_login_count_threshold()
    brute_time_gap_th = config.get_brute_force_time_gap_threshold()

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

    analyzer = LogAnalyzer(brute_login_count_th, brute_time_gap_th)

    # fdf.show()

    analyzer.start_analysis(df, "","")
    # df.show()

    # ip_count = df.groupby("status").count()
    # ip_count.show()

