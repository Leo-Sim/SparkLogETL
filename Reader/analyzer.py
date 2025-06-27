
from pyspark.sql.functions import col
from pyspark.sql import DataFrame

from Reader.log_info import LogField

class LogAnalyzer:
    def __init__(self):
        pass


    def save_result(self, df, output_path, file_name) -> None:
        """
        Save the analysis result
        :param df:
        :param output_path:
        :param file_name:
        :return:
        """
        pass


    def filter_by_status(self, df: DataFrame, status):
        return df.filter(col(LogField.LOG_FIELD_STATUS) == status)



