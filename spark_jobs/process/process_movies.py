from pyspark.sql.functions import *
from pyspark.sql.types import *




def run_fact(spark, config, files_url, table_name="fact_movies", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):
    df_raw = spark.read.format("json")\
                        .option("multiline", "true")\
                        .option("inferSchema", "true")\
                        .load(files_url) # MOVIE_13 para pruebas

    fact_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    df_movies = df_raw.select(
        col("id").alias("movie_id"),
        col("title"),
        col("tagline"),
        col("overview"),
        col("runtime").cast("int"),
        col("budget").cast("bigint"),
        col("revenue").cast("bigint"),
        col("release_date").try_cast("date"),
        col("vote_average").cast("float"),
        col("vote_count").cast("int"),
        col("homepage"),
        col("imdb_id"),
        col("original_language"),
        col("popularity").cast("float"),
        col("poster_path"),
        col("backdrop_path")
    )

    df_to_insert = df_movies.alias("new") \
        .join(fact_existing_df.alias("existing"),
            on=col("new.movie_id") == col("existing.movie_id"),
            how="left_anti"
        )


    df_to_insert = df_to_insert.coalesce(4)
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


   