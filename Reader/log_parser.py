from abc import ABC, abstractmethod
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import regexp_extract, regexp

import logging

class LogParserFactory:
    """
    This class returns parser class according to its log pattern
    """

    LOG_TYPE_APACH_WEB_LOG = 1

    @staticmethod
    def get_log_parser(log_type):

        """
        This function returns parser class according to its log pattern
        :param log_type:
        :return:
        """
        if log_type == LogParserFactory.LOG_TYPE_APACH_WEB_LOG:
            return ApacheWebLogParser()


        else:
            logging.error("Log type is not supported")
            return None




class LogParser(ABC):

    @abstractmethod
    def parse_raw_log(self, df) -> DataFrame :
        """
        This function is based methods for parsing logs.
        It takes data frame and return parsed data frame
        :param df:
        :return:
        """
        pass


class ApacheWebLogParser(LogParser):
    """
    This class parses apache web logs
    """
    apache_web_log_regex = r'(\S+) - - \[(.*?)\] "(.*?) (.*?) (.*?)" (\d{3}) (\d+) "(.*?)" "(.*?)" "(.*?)"'

    def parse_raw_log(self, df) -> DataFrame :
        parsed_df = df.select(
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 1).alias("ip"),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 2).alias("time_stamp"),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 3).alias("method"),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 4).alias("url"),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 5).alias("protocol"),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 6).alias("status"),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 7).alias("size"),

        )

        return parsed_df



