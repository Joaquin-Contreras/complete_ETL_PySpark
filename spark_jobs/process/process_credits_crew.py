from pyspark.sql.functions import *
from pyspark.sql.types import *


def run_fact(spark, config, files_url, table_name="fact_credits_crew", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):

    df_raw = spark.read.format("json")\
                    .option("multiline", "true")\
                    .option("inferSchema", "true")\
                    .load(files_url)

    df_credits_crew = df_raw.select(
        col("id").alias("movie_id"),
        explode(col("credits.crew")).alias("crew")
    ).select(
        col("crew.credit_id").alias("credit_id"),
        col("movie_id").alias("movie_id"),
        col("crew.gender").alias("gender"),
        col("crew.known_for_department").alias("known_for_department"),
        col("crew.name").alias("name"),
        col("crew.original_name").alias("original_name"),
        col("crew.popularity").alias("popularity"),
        col("crew.profile_path").alias("profile_path"),
        col("crew.department").alias("department"),
        col("crew.job").alias("job")
    )

    fact_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    df_to_insert = df_credits_crew.alias("new") \
        .join(fact_existing_df.alias("existing"),
            on=[col("new.movie_id") == col("existing.movie_id"),
                col("new.credit_id") == col("existing.credit_id")],
            how="left_anti"
        )
        
    df_to_insert = df_to_insert.coalesce(4)
    # Escribir solo los nuevos
    df_to_insert.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .option("batchsize", 500) \
        .option("isolationLevel", "NONE") \
        .option("truncate", "false") \
        .mode("append") \
        .save()

    












