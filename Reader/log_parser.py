from abc import ABC, abstractmethod
from ast import parse

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import regexp_extract, regexp, udf, col
from pyspark.sql.types import StringType

from datetime import datetime

from Reader.log_info import LogField

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

    # @abstractmethod
    # def change_date_format(self, date) -> str:
    #     """
    #     This function change each type of date to "YYYY-MM-DDThh:mm:ss"
    #     :param date:
    #     :return:
    #     """



class ApacheWebLogParser(LogParser):
    """
    This class parses apache web logs
    """
    apache_web_log_regex = r'(\S+) - - \[(.*?)\] "(.*?) (.*?) (.*?)" (\d{3}) (\d+) "(.*?)" "(.*?)" "(.*?)"'

    def parse_raw_log(self, df: DataFrame) -> DataFrame :



        parsed_df = df.select(
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 1).alias(LogField.LOG_FIELD_IP),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 2).alias(LogField.LOG_FIELD_TIMESTAMP),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 3).alias(LogField.LOG_FIELD_METHOD),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 4).alias(LogField.LOG_FIELD_URL),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 5).alias(LogField.LOG_FIELD_PROTOCOL),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 6).alias(LogField.LOG_FIELD_STATUS),
            regexp_extract("value", ApacheWebLogParser.apache_web_log_regex, 7).alias(LogField.LOG_FIELD_SIZE),

        )

        date_format = LogField.DATE_TIME_FORMAT

        # This code is for changing date format to ISO type
        def change_date(date_str: str) -> str:
            try:
                dt = datetime.strptime(date_str, "%d/%b/%Y:%H:%M:%S %z")
                return dt.strftime(date_format)
            except Exception:
                return None

        # Change date type
        date_changer_udf = udf(change_date, StringType())
        parsed_df = parsed_df.withColumn(LogField.LOG_FIELD_TIMESTAMP, date_changer_udf(col(LogField.LOG_FIELD_TIMESTAMP)))

        return parsed_df





