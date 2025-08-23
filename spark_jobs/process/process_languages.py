from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import ArrayType, StructType


def run_dim(spark, config, files_url, table_name="dim_language", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):
    df_raw = spark.read.format("json")\
                        .option("multiline", "true")\
                        .option("inferSchema", "true")\
                        .load(files_url)

    if "spoken_languages" in df_raw.columns:
        spoken_languages_type = df_raw.schema["spoken_languages"].dataType

        if isinstance(spoken_languages_type, ArrayType):
            if isinstance(spoken_languages_type.elementType, StructType):
                pass 
            else:
                return
        else:
            return
    else:
        return

    df_languages_raw = df_raw.select(
        explode(col("spoken_languages")).alias("language")
    )
    df_spoken_languages = df_languages_raw.select(
        col("language.iso_639_1").alias("iso_639_1"),
        col("language.english_name").alias("english_name")
    )

    # Leer dimensión existente
    dim_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    # Crear nuevo dataframe sin duplicados existentes
    df_spoken_languages_clean = df_spoken_languages.dropDuplicates(["iso_639_1"])

    df_to_insert = df_spoken_languages_clean.alias("new") \
        .join(dim_existing_df.alias("existing"),
            on=col("new.iso_639_1") == col("existing.iso_639_1"),
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

def run_fact(spark, config, files_url, table_name="fact_movie_language", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):

    # Leer dim_language y fact_movies desde MySQL
    dim_language_df = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/tmdb_database") \
        .option("dbtable", "dim_language") \
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

    # Leer JSON crudo
    movies_raw = spark.read.format("json")\
                        .option("multiline", "true")\
                        .option("inferSchema", "true")\
                        .load(files_url)

    if "spoken_languages" in movies_raw.columns:
        prod_companies_type = movies_raw.schema["spoken_languages"].dataType

        if isinstance(prod_companies_type, ArrayType):
            if isinstance(prod_companies_type.elementType, StructType):
                pass 
            else:
                return
        else:
            return
    else:
        return

    # Explode spoken_languages y extraer language_code
    movies_languages = movies_raw.select(
        col("id").alias("movie_id"),  # este es el que matchea con fact_movies
        explode("spoken_languages").alias("language")
    ).select(
        col("movie_id"),
        col("language.iso_639_1").alias("language_code")
    )

    # Join con fact_movies y dim_language
    fact_lang_df = movies_languages \
        .join(
            fact_movies_df.select("movie_id"),  # ya tiene el mismo nombre
            on="movie_id",
            how="inner"
        ) \
        .join(
            dim_language_df.select(
                col("iso_639_1"),
                col("id").alias("language_id")
            ),
            on=[col("language_code") == col("iso_639_1")],
            how="inner"
        ) \
        .select("movie_id", "language_id") \
        .dropDuplicates()

    fact_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    fact_lang_df = fact_lang_df.dropDuplicates(["movie_id", "language_id"])

    fact_to_insert = fact_lang_df.alias("new") \
        .join(fact_existing_df.alias("existing"),
            on=["movie_id", "language_id"],
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