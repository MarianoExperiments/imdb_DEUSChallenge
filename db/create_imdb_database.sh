#!/bin/bash
set -e

# Execute the SQL scripts
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    
    CREATE SCHEMA imdb;

    
    CREATE TABLE imdb.title_basics (
        tconst VARCHAR(255) PRIMARY KEY,
        titleType VARCHAR(255),
        primaryTitle VARCHAR(255),
        originalTitle VARCHAR(255),
        isAdult BOOLEAN,
        startYear INT,
        endYear INT,
        runtimeMinutes INT,
        genres VARCHAR(255)
    );


    CREATE TABLE imdb.title_principals (
        tconst VARCHAR(255) PRIMARY KEY,
        ordering INT,
        nconst VARCHAR(255),
        category VARCHAR(255),
        job VARCHAR(255),
        characters VARCHAR(255)
    );

    
    CREATE TABLE imdb.title_ratings (
        tconst VARCHAR(255) PRIMARY KEY,
        averageRating FLOAT,
        numVotes INT
    );

    
    CREATE TABLE imdb.name_basics (
        nconst VARCHAR(255) PRIMARY KEY,
        primaryName VARCHAR(255),
        birthYear INT,
        deathYear INT,
        primaryProfession VARCHAR(255),
        tconst VARCHAR(255)
    );


    CREATE TABLE imdb.professional_info (
        name VARCHAR(255),
        tconst VARCHAR(255),
        category VARCHAR(255),
        runtimeMinutes VARCHAR(255),
        averageRating FLOAT
    );

EOSQL

