# from pyspark.sql import SparkSession
# import os


# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# driver_path = os.path.join(BASE_DIR,".." , "lib", "mysql-connector-j-9.4.0.jar")
# os.environ["HADOOP_OPTS"] = "-Djava.library.path="
# def create_spark_session(app_name: str = "TMDB-Pipeline") -> SparkSession:
#     return (
#         SparkSession.builder
#         .appName(app_name)
#         .config("spark.hadoop.io.native.lib.available", "false") \
#         .config("spark.hadoop.native.lib", "false") \
#         .config("spark.jars", driver_path) \
#         .config("spark.driver.extraClassPath", driver_path) \
#         .getOrCreate()
#     )

from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "TMDB-Pipeline") -> SparkSession:
    driver_path = "/opt/bitnami/spark/jars/mysql-connector-j-9.4.0.jar"
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.jars", driver_path)
        .config("spark.driver.extraClassPath", driver_path)
        .getOrCreate()
    )
# Note: The driver_path is set to the location where the MySQL connector JAR is expected to be in the Bitnami Spark Docker image.