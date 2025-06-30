
from pyspark.sql.functions import col, min, max, unix_timestamp, floor, lit
from pyspark.sql import DataFrame

from Reader.log_info import LogField
from datetime import datetime

class LogAnalyzer:
    """
    Base class for analyzing logs

    """


    ATTACK_BRUTE_FORCE = "Brute force"

    def __init__(self, brute_count_th, brute_time_th):

        self.brute_count_th = brute_count_th
        self.brute_time_th = brute_time_th

    def save_result(self, df, output_path, file_name) -> None:
        """
        Save the analysis result
        :param df:
        :param output_path:
        :param file_name:
        :return:
        """
        pass


    def start_analysis(self, df: DataFrame, output_path, file_name):
        brute_force_df = self._detect_brute_force(df)

        result = brute_force_df.toJSON().collect()

        print(result)




    def _filter_by_ip(self, df: DataFrame, ip) -> DataFrame:
        return df.filter(col(LogField.LOG_FIELD_IP) == ip)

    def _filter_by_status(self, df: DataFrame, status) -> DataFrame:
        return df.filter(col(LogField.LOG_FIELD_STATUS) == status)

    def _filter_by_method(self, df: DataFrame, method) -> DataFrame:
        return df.filter(col(LogField.LOG_FIELD_METHOD) == method)

    def _filter_by_protocol(self, df: DataFrame, protocol) -> DataFrame:
        return df.filter(col(LogField.LOG_FIELD_PROTOCOL) == protocol)

    def _detect_brute_force(self, df: DataFrame) -> DataFrame:

        BUCKET = "bucket"


        _df = df.select(
            min(col(LogField.LOG_FIELD_TIMESTAMP)).alias("min")
        ).collect()[0]

        min_date = datetime.strptime(_df["min"], LogField.DATE_TIME_FORMAT)

        bucket_df = df.withColumn(
            BUCKET,
            floor((unix_timestamp(LogField.LOG_FIELD_TIMESTAMP) - lit(int(min_date.timestamp()))) / lit(self.brute_time_th))
        )

        result = (bucket_df
                  .filter(col(LogField.LOG_FIELD_URL).rlike("(?i).*login.*"))
                  .groupby(BUCKET, LogField.LOG_FIELD_IP)
                  .count()
                  .filter(col("count") >= self.brute_count_th)
                  .withColumn(LogField.ATTACK_TYPE, lit(LogAnalyzer.ATTACK_BRUTE_FORCE))
                  .join(bucket_df, on=["bucket", "ip"], how="left")
                  .dropDuplicates([LogField.LOG_FIELD_IP, LogField.LOG_FIELD_URL])
                  .orderBy(col("count").desc())
                  .drop("count"))

        return result



















