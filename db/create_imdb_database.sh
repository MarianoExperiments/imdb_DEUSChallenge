#!/bin/bash
set -e

# Execute the SQL scripts
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    
    CREATE SCHEMA imdb;

    
    CREATE TABLE imdb.title_basics (
        tconst VARCHAR(255) PRIMARY KEY,
        titleType text,
        primaryTitle text,
        originalTitle text,
        isAdult BOOLEAN,
        startYear INT,
        endYear INT,
        runtimeMinutes INT,
        genres text
    );


    CREATE TABLE imdb.title_principals (
        tconst VARCHAR(255),
        ordering INT,
        nconst VARCHAR(255),
        category text,
        job text,
        characters text
    );

    
    CREATE TABLE imdb.title_ratings (
        tconst VARCHAR(255) PRIMARY KEY,
        averageRating FLOAT,
        numVotes INT
    );

    
    CREATE TABLE imdb.name_basics (
        nconst VARCHAR(255),
        primaryName text,
        birthYear INT,
        deathYear INT,
        primaryProfession text,
        tconst VARCHAR(255)
    );

    CREATE MATERIALIZED VIEW imdb.actor_movie_details AS
        WITH tb_tr AS (
            SELECT tb.tconst, tb.runtimeminutes, tr.averagerating
            FROM imdb.title_basics AS tb
            INNER JOIN imdb.title_ratings AS tr ON tb.tconst = tr.tconst
            WHERE tb.runtimeminutes IS NOT NULL
        ), nb_tp AS (
            SELECT DISTINCT nb.primaryname AS name, nb.tconst, category
            FROM imdb.name_basics AS nb
            INNER JOIN imdb.title_principals AS tp ON (nb.tconst = tp.tconst AND nb.nconst = tp.nconst)
            WHERE (tp.category IN ('actor', 'actress')) AND (nb.primaryname ILIKE '%actor%' OR nb.primaryname ILIKE '%actress%')
        )
        SELECT nb_tp.name, nb_tp.tconst, nb_tp.category, tb_tr.runtimeminutes, tb_tr.averagerating
        FROM nb_tp
        INNER JOIN tb_tr ON tb_tr.tconst = nb_tp.tconst;


EOSQL

