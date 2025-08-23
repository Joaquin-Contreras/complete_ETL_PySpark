from pyspark.sql.functions import *
from pyspark.sql.types import *




def run_dim(spark, config, files_url, table_name="dim_reviews", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):
    
    df_raw = spark.read.format("json")\
                        .option("multiline", "true")\
                        .option("inferSchema", "true")\
                        .load(files_url)

    if "reviews" in df_raw.columns:
        reviews_field = df_raw.schema["reviews"]
        if "results" in reviews_field.dataType.names:
            results_type = reviews_field.dataType["results"]

            if isinstance(results_type, ArrayType):
                df_reviews = df_raw.select(explode(col("reviews.results")).alias("review"))
            else:
                return
        else:
            return
    else:
        return

    df_reviews = df_raw.select(
        explode(col("reviews.results")).alias("reviews")
    ).select(
        col("reviews.id").alias("review_id"),
        col("reviews.content").alias("content"),
        col("reviews.created_at").astype("DATE").alias("created_at"),
        col("reviews.url").alias("url")
    ).dropDuplicates(["review_id"])

    df_reviews.count()


    dim_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()
    

    df_to_insert = df_reviews.alias("new") \
        .join(dim_existing_df.alias("existing"),
            on=col("new.review_id") == col("existing.review_id"),
            how="left_anti")


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



def run_fact(spark, config, files_url, table_name="fact_reviews", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):

    dim_reviews_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/tmdb_database") \
        .option("dbtable", "dim_reviews") \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    fact_movies_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/tmdb_database") \
        .option("dbtable", "fact_movies") \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    movies_raw = spark.read.format("json")\
                            .option("multiline", "true")\
                            .option("inferSchema", "true")\
                            .load(files_url) # MOVIE_13 para pruebas
    
    if "reviews" in movies_raw.columns:
        reviews_field = movies_raw.schema["reviews"]
        if "results" in reviews_field.dataType.names:
            results_type = reviews_field.dataType["results"]

            if isinstance(results_type, ArrayType):
                df_reviews = movies_raw.select(explode(col("reviews.results")).alias("review"))
            else:
                return
        else:
            return
    else:
        return

    movies_reviews = movies_raw.select(
        explode(col("reviews.results")).alias("reviews"),
        col("id").alias("movie_id")
    ).select(
        col("reviews.id").alias("review_id"),
        col("movie_id")
    ).dropDuplicates(["review_id", "movie_id"])

    fact_reviews_df = movies_reviews \
        .join(
            fact_movies_df.select("movie_id"),
            on="movie_id",
            how="inner"
        ) \
        .join(
            dim_reviews_df.select("review_id"),
            on="review_id",
            how="inner"
        ).select("review_id", "movie_id")

    fact_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    fact_to_insert = fact_reviews_df \
        .join(fact_existing_df,
            on=["review_id", "movie_id"],
            how="left_anti")


    fact_to_insert = fact_to_insert.coalesce(4)
    fact_to_insert.write \
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