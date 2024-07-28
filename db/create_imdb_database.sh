#!/bin/bash
set -e

mkdir -p -m 777 /mnt/datalake/raw/imdb
mkdir -p -m 777 /mnt/datalake/clean/imdb

# Execute the SQL scripts
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    CREATE TABLESPACE imdb_space LOCATION '/mnt/datalake/clean/imdb';

    CREATE SCHEMA imdb;
    
    CREATE TABLE imdb.title_akas (
        titleId VARCHAR(255) PRIMARY KEY,
        ordering INT,
        title VARCHAR(255),
        region VARCHAR(255),
        language VARCHAR(255),
        types VARCHAR(255), 
        attributes VARCHAR(255),
        isOriginalTitle BOOLEAN
    )TABLESPACE imdb_space;

    CREATE TABLE imdb.title_basics (
        tconst VARCHAR(255) PRIMARY KEY,
        titleType VARCHAR(255),
        primaryTitle VARCHAR(255),
        originalTitle VARCHAR(255),
        isAdult BOOLEAN,
        startYear INT,
        endYear INT,
        runtimeMinutes INT,
        genres VARCHAR(5)
    )TABLESPACE imdb_space;

    CREATE TABLE imdb.title_crew (
        tconst VARCHAR(255) PRIMARY KEY,
        directors VARCHAR(255),
        writers VARCHAR(255) 
    )TABLESPACE imdb_space;

    CREATE TABLE imdb.title_episode (
        tconst VARCHAR(255) PRIMARY KEY,
        parentTconst VARCHAR(255),
        seasonNumber INT,
        episodeNumber INT
    )TABLESPACE imdb_space;

    CREATE TABLE imdb.title_principals (
        tconst VARCHAR(255) PRIMARY KEY,
        ordering INT,
        nconst VARCHAR(255),
        category VARCHAR(255),
        job VARCHAR(255),
        characters VARCHAR(255)
    )TABLESPACE imdb_space;

    CREATE TABLE imdb.title_ratings (
        tconst VARCHAR(255) PRIMARY KEY,
        averageRating FLOAT,
        numVotes INT
    )TABLESPACE imdb_space;

    CREATE TABLE imdb.name_basics (
        nconst VARCHAR(255) PRIMARY KEY,
        primaryName VARCHAR(255),
        birthYear INT,
        deathYear INT,
        primaryProfession VARCHAR(255),
        knownForTitles VARCHAR(255)
    )TABLESPACE imdb_space;

EOSQL

