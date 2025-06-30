
from pyspark.sql import SparkSession

class Connector:
    def __init__(self, server_address):
        self.server_address = server_address

    def connect(self):
        """

            Generate the information local or remote server
            Currently local connection is supported
            #TODO: add remote connection
            """

        SparkSession.builder.master("local[*]").getOrCreate().stop()
        spark = SparkSession.builder.remote("sc://" + self.server_address).getOrCreate()

        self.session = spark
        return self

    def get_session(self):
        return self.session