
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from requests import session
from pathlib import Path
from Reader.log_parser import LogParserFactory


class LogReader:

    """
        This class is for reading raw logs.
    """
    def __init__(self, spark: SparkSession, dir_path):

        self.idx = 0
        self.dir_path = dir_path
        self.spark = spark



    def read_file(self):
        """
        This function reads text log file
        :return:
        """

        if os.name == "nt":
            file_name = "access.log"
            file_path = self.dir_path + "/" + file_name

            # set parser
            parser = LogParserFactory.get_log_parser(LogParserFactory.LOG_TYPE_APACH_WEB_LOG)
            df = self.spark.read.text(file_path)
            parsed_df = parser.parse_raw_log(df)
            parsed_df.show()


        else:
            file_list = os.listdir(self.dir_path)
            for file_name in file_list:
                file_path = Path(os.path.join(self.dir_path, file_name)).as_uri()
                print("file path : ",file_path)
                df = self.spark.read.text(file_path)
                df.show()
                # log_rdd = spark..textFile("file:///path/to/access.log")


    def read_file_manually(self, batch_size) -> list:
        """
        Manually Read all csv and txt files in the directory and return lines of logs

        :param batch_size:
        :return: list of log strings
        """

        file_list = os.listdir(self.dir_path)

        for file_name in file_list:
            file_path = os.path.join(self.dir_path, file_name)
            with open(file_path, mode='r') as file:
                batch_list = []

                for line in file:
                    batch_list.append(line)

                    if len(batch_list) == batch_size:
                        print("batch list : ", batch_list)
                        yield batch_list
                        batch_list = []

