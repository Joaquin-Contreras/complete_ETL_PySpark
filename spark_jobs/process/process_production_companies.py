from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.types import ArrayType, StructType




def run_dim(spark, config, files_url, table_name="dim_production_companies", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):
    df_raw = spark.read.format("json")\
                        .option("multiline", "true")\
                        .option("inferSchema", "true")\
                        .load(files_url)


    if "production_companies" in df_raw.columns:
        prod_companies_type = df_raw.schema["production_companies"].dataType

        if isinstance(prod_companies_type, ArrayType):
            if isinstance(prod_companies_type.elementType, StructType):
                pass 
            else:
                return
        else:
            return
    else:
        return

    df_production_companies = df_raw.select(
        explode(col("production_companies")).alias("production_companies")
    ).select(
        col("production_companies.logo_path").alias("logo_path"),
        col("production_companies.name").alias("name"),
        col("production_companies.origin_country").alias("origin_country")
    ).dropDuplicates(["name"])

    dim_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load()

    df_to_insert = df_production_companies.alias("new") \
        .join(dim_existing_df.alias("existing"),
            on=col("new.name") == col("existing.name"),
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

def run_fact(spark, config, files_url, table_name="fact_production_companies", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database"):

    dim_production_companies = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:mysql://mysql:3306/tmdb_database") \
        .option("dbtable", "dim_production_companies") \
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
                            .load(files_url)


    if "production_companies" in movies_raw.columns:
        prod_companies_type = movies_raw.schema["production_companies"].dataType

        if isinstance(prod_companies_type, ArrayType):
            if isinstance(prod_companies_type.elementType, StructType):
                pass 
            else:
                return
        else:
            return
    else:
        return

    movies_production_companies = movies_raw.select(
        explode(col("production_companies")).alias("production_companies"),
        col("id").alias("movie_id")
    ).select(
        col("production_companies.name").alias("company_name"),
        col("movie_id")
    )

    fact_production_companies_df = movies_production_companies \
        .join(
            fact_movies_df.alias("movie_id"),
            on="movie_id",
            how="inner"
        ) \
        .join(
            dim_production_companies.alias("company_name"),
            on=col("company_name") == col("name"),
            how="inner"
        ) \
        .select(
            col("movie_id").alias("movie_id"),
            col("id").alias("company_id"),
        )

    fact_production_companies_df = fact_production_companies_df.dropna(subset=["movie_id", "company_id"])
    fact_production_companies_df = fact_production_companies_df.dropDuplicates(["movie_id", "company_id"])

            
    fact_existing_df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", config['user']) \
        .option("password", config['password']) \
        .load() \
        .dropDuplicates(["movie_id", "company_id"])

    fact_existing_df = fact_existing_df.persist()
    fact_existing_df.count()
    

    fact_to_insert = fact_production_companies_df.alias("new") \
        .join(fact_existing_df.alias("existing"),
            on=[col("new.movie_id") == col("existing.movie_id"),
                col("new.company_id") == col("existing.company_id")],
            how="left_anti"
        )    
    
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