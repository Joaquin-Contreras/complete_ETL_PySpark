from utils.spark_session import create_spark_session
from spark_jobs.process import process_languages, process_genres, process_reviews, process_production_companies, process_movies, process_videos, process_credits_cast, process_credits_crew
from dotenv import load_dotenv
import os
import glob

load_dotenv() 


# BASE_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

file_path = os.path.join(BASE_DIR, "..", "data", "raw")
# Convierte los backslashes a forward slashes
file_path = file_path.replace("\\", "/")
# Usa glob para expandir los archivos
file_path = glob.glob(os.path.join(file_path, "*.json"))


if not file_path:
    print("No se encontraron archivos JSON en:", file_path)
    exit()



# get env variables credentials
password = os.getenv("MYSQL_PASSWORD")
user = os.getenv("MYSQL_USER")

if not password:
    raise ValueError("MYSQL_PASSWORD environment variable is not set.")
if not user:
    raise ValueError("MYSQL_USER environment variable is not set.")


config = {
    "user": user,
    "password": password  
}

def main():

    os.environ["HADOOP_OPTS"] = "-Djava.library.path="

    spark = create_spark_session().builder.config("spark.sql.shuffle.partitions", "4").getOrCreate()

    try:
        print("Inserting language...")
        process_languages.run_dim(spark, config, table_name="dim_language", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)

        print("Inserting genre...")
        process_genres.run_dim(spark, config, table_name="dim_genre", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)

        print("Inserting reviews...")
        process_reviews.run_dim(spark, config, table_name="dim_reviews", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)    

        print("Inserting production companies...")
        process_production_companies.run_dim(spark, config, table_name="dim_production_companies", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)

        print("Inserting movies...")
        process_movies.run_fact(spark, config, table_name="fact_movies", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)

        print("Inserting fact languages...")
        process_languages.run_fact(spark, config, table_name="fact_movie_language", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)

        print("Inserting fact genres...")
        process_genres.run_fact(spark, config, table_name="fact_movie_genre", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)


        print("Inserting fact reviews...")
        process_reviews.run_fact(spark, config, table_name="fact_reviews", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)


        print("Inserting fact production companies...")
        process_production_companies.run_fact(spark, config, table_name="fact_production_companies", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)


        print("Inserting fact videos...")
        process_videos.run_fact(spark, config, table_name="fact_videos", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)


        print("Inserting fact credits cast...")
        process_credits_cast.run_fact(spark, config, table_name="fact_credits_cast", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)


        print("Inserting fact credits crew...")
        process_credits_crew.run_fact(spark, config, table_name="fact_credits_crew", jdbc_url="jdbc:mysql://mysql:3306/tmdb_database", files_url=file_path)



        print("Pipeline executed successfully.")
    except Exception as e:
        print(f"Error during the pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()