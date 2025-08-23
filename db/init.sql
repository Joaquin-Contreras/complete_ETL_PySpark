CREATE DATABASE IF NOT EXISTS tmdb_database;
USE tmdb_database;

-- Tabla principal de películas
CREATE TABLE fact_movies (
  movie_id INT PRIMARY KEY NOT NULL,
  title VARCHAR(255) NOT NULL,
  tagline TEXT,
  overview TEXT,
  runtime INT NOT NULL,
  budget BIGINT,
  revenue BIGINT,
  release_date DATE,
  launched BOOLEAN,
  vote_average FLOAT,
  vote_count INT,
  homepage VARCHAR(255),
  imdb_id VARCHAR(20),
  original_language VARCHAR(10),
  popularity FLOAT,
  poster_path VARCHAR(255),
  backdrop_path VARCHAR(255)
);

-- Tabla de videos relacionados a películas
CREATE TABLE fact_videos (
  video_id VARCHAR(255) PRIMARY KEY NOT NULL,
  movie_id INT NOT NULL,
  name VARCHAR(255),
  site VARCHAR(100),
  size INT,
  type VARCHAR(100),
  official BOOLEAN,
  published_at DATE,
  `key` VARCHAR(100),
  iso_639_1 VARCHAR(10),
  FOREIGN KEY (movie_id) REFERENCES fact_movies(movie_id)
);


-- Cast (actores) por película
CREATE TABLE fact_credits_cast (
  id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
  credit_id VARCHAR(100),
  movie_id INT NOT NULL,
  cast_id INT,
  character_name VARCHAR(255),
  name VARCHAR(255),
  original_name VARCHAR(255),
  known_for_department VARCHAR(100),
  popularity FLOAT,
  profile_path VARCHAR(255),
  FOREIGN KEY (movie_id) REFERENCES fact_movies(movie_id)
);

-- Crew (directores, escritores, etc.) por película
CREATE TABLE fact_credits_crew (
  id INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
  credit_id VARCHAR(100),
  movie_id INT,
  gender INT,
  known_for_department VARCHAR(100),
  name VARCHAR(255),
  original_name VARCHAR(255),
  popularity FLOAT,
  profile_path VARCHAR(255),
  department VARCHAR(100),
  job VARCHAR(100),
  FOREIGN KEY (movie_id) REFERENCES fact_movies(movie_id)
);

-- Idiomas disponibles
CREATE TABLE dim_language (
  id INT PRIMARY KEY AUTO_INCREMENT,
  iso_639_1 VARCHAR(10) UNIQUE,
  english_name VARCHAR(100)
);

-- Relación película - idioma
CREATE TABLE fact_movie_language (
  movie_id INT NOT NULL,
  language_id INT NOT NULL,
  PRIMARY KEY (movie_id, language_id),
  FOREIGN KEY (movie_id) REFERENCES fact_movies(movie_id),
  FOREIGN KEY (language_id) REFERENCES dim_language(id)
);

-- Géneros
CREATE TABLE dim_genre (
  id INT PRIMARY KEY NOT NULL,
  name VARCHAR(100)
);

-- Relación película - género
CREATE TABLE fact_movie_genre (
  movie_id INT NOT NULL,
  genre_id INT NOT NULL,
  PRIMARY KEY (movie_id, genre_id),
  FOREIGN KEY (movie_id) REFERENCES fact_movies(movie_id),
  FOREIGN KEY (genre_id) REFERENCES dim_genre(id)
);

-- Reseñas únicas
CREATE TABLE dim_reviews (
  id INT PRIMARY KEY AUTO_INCREMENT,
  review_id VARCHAR(100) UNIQUE,
  content TEXT,
  created_at DATE,
  url VARCHAR(255)
);

-- Relación película - reseña
CREATE TABLE fact_reviews (
  movie_id INT NOT NULL,
  review_id VARCHAR(255) NOT NULL,
  PRIMARY KEY (movie_id, review_id),
  FOREIGN KEY (movie_id) REFERENCES fact_movies(movie_id),
  FOREIGN KEY (review_id) REFERENCES dim_reviews(review_id)
);

-- Compañías productoras
CREATE TABLE dim_production_companies (
  id INT PRIMARY KEY AUTO_INCREMENT,
  logo_path VARCHAR(255),
  name VARCHAR(255),
  origin_country VARCHAR(10)
);

-- Relación película - compañía
CREATE TABLE fact_production_companies (
  movie_id INT NOT NULL,
  company_id INT NOT NULL,
  PRIMARY KEY (movie_id, company_id),
  FOREIGN KEY (movie_id) REFERENCES fact_movies(movie_id),
  FOREIGN KEY (company_id) REFERENCES dim_production_companies(id)
);

