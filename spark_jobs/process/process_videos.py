from pyspark.sql.functions import *
from pyspark.sql.types import *



def run_fact(spark, config, files_url, table_name="fact_videos", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):

    df_raw = spark.read.format("json")\
                    .option("multiline", "true")\
                    .option("inferSchema", "true")\
                    .load(files_url)
    if "videos" in df_raw.columns:
        videos_field = df_raw.schema["videos"]
        if "results" in videos_field.dataType.names:
            results_type = videos_field.dataType["results"]

            if isinstance(results_type, ArrayType):
                df_videos = df_raw.select(explode(col("videos.results")).alias("video"))
            else:
                return
        else:
            return
    else:
        return

    df_videos = df_raw.select(
        col("id").alias("movie_id"),
        explode(col("videos.results")).alias("video")
    ).select(
        col("movie_id"),
        col("video.id").alias("video_id"),
        col("video.name").alias("name"),
        col("video.site").alias("site"),
        col("video.size").alias("size"),
        col("video.type").alias("type"),
        col("video.official").alias("official"),
        col("video.published_at").cast("DATE").alias("published_at"),
        col("video.key").alias("key"),
        col("video.iso_639_1").alias("iso_639_1"),
    ).dropDuplicates(["movie_id", "video_id"])





    fact_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    df_to_insert = df_videos.alias("new") \
        .join(fact_existing_df.alias("existing"),
            on=[col("new.movie_id") == col("existing.movie_id"),
                col("new.video_id") == col("existing.video_id")],
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













