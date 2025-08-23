from pyspark.sql.functions import *
from pyspark.sql.types import *




def run_dim(spark, config, files_url, table_name="dim_genre", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):
    df_raw = spark.read.format("json")\
                        .option("multiline", "true")\
                        .option("inferSchema", "true")\
                        .load(files_url)

    if "genres" in df_raw.columns:
        genres_type = df_raw.schema["genres"].dataType

        if isinstance(genres_type, ArrayType):
            if isinstance(genres_type.elementType, StructType):
                pass 
            else:
                return
        else:
            return
    else:
        return

    
    df_raw = df_raw.select(
        explode(col("genres")).alias("genres")
    )

    df_genres = df_raw.select(
        col("genres.id").alias("id"),
        col("genres.name").alias("name")
    )

    # Leer dimensión existente
    dim_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    df_genres_clean = df_genres.dropDuplicates(["id"])

    df_to_insert = df_genres_clean.alias("new") \
        .join(dim_existing_df.alias("existing"),
            on=col("new.id") == col("existing.id"),
            how="left_anti")

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


def run_fact(spark, config, files_url, table_name="fact_movie_genre", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):
    
    dim_genre_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/tmdb_database") \
        .option("dbtable", "dim_genre") \
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

    df_raw = spark.read.format("json")\
                        .option("multiline", "true")\
                        .option("inferSchema", "true")\
                        .load(files_url) 

    if "genres" in df_raw.columns:
        genres_type = df_raw.schema["genres"].dataType

        if isinstance(genres_type, ArrayType):
            if isinstance(genres_type.elementType, StructType):
                pass 
            else:
                return
        else:
            return
    else:
        return


    df_genres = df_raw.select(
        explode(col("genres")).alias("genres"),
        col("id").alias("movie_id")
    ).select(
        col("genres.id").alias("genre_id"),
        col("movie_id")
    )

    fact_genre_df = df_genres \
        .join(
            fact_movies_df.alias("movie_id"),
            on="movie_id",
            how="inner"
        ) \
        .join(
            dim_genre_df.alias("genre_id"),
            on=col("genre_id") == col("id"),
            how="inner"
        ) \
        .select(
            col("movie_id").alias("movie_id"),
            col("genre_id").alias("genre_id")
        ).dropDuplicates(["movie_id", "genre_id"])
    
    fact_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    fact_to_insert = fact_genre_df.alias("new") \
        .join(fact_existing_df.alias("existing"),
            on=[col("new.movie_id") == col("existing.movie_id"), 
                col("new.genre_id") == col("existing.genre_id")],
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