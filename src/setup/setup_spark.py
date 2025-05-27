from pyspark.sql import SparkSession


class SetupSpark:
    def __init__(self, app_name="MySparkApp"):
        self.app_name = app_name
        self.spark = None

    def create_session(self):
        builder = (
            SparkSession.builder.appName(self.app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "4g")
        )

        self.spark = builder.getOrCreate()
        return self.spark

    def stop_session(self):
        if self.spark:
            self.spark.stop()
            self.spark = None
