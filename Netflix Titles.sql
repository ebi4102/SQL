DROP TABLE IF EXISTS netflix_titles;

CREATE TABLE netflix_titles(
show_id VARCHAR(10),
show_type VARCHAR(50),
title VARCHAR(1000),
director VARCHAR(1000),
cast_member VARCHAR(2500),
country VARCHAR(1000),
date_added VARCHAR(1000),
release_year VARCHAR(25),
rating VARCHAR(25),
duration VARCHAR(50),
listed_in VARCHAR(1000),
description VARCHAR(25000));

COPY netflix_titles FROM 'D:\postsql\netflix_titles.csv' WITH CSV HEADER; 

SELECT * FROM netflix_titles

DELETE FROM netflix_titles
WHERE show_id IS NULL OR show_type IS NULL OR title IS NULL OR director IS NULL OR cast_member IS NULL OR country IS NULL OR date_added IS NULL OR
release_year IS NULL OR rating IS NULL OR duration IS NULL OR listed_in IS NULL OR description IS NULL;

--Count the number of Movies and TV Shows:
SELECT show_type, COUNT(*) AS count
FROM netflix_titles
GROUP BY show_type;

--Find the number of movies released each year:
SELECT release_year, COUNT(*) AS movie_count
FROM netflix_titles
WHERE show_type = 'Movie'
GROUP BY release_year
ORDER BY release_year DESC;


--Get the top 5 countries with the most shows:
SELECT country, COUNT(*) AS show_count
FROM netflix_titles
GROUP BY country
ORDER BY show_count DESC
LIMIT 5;

-- Find the movies with the longest duration:
WITH formatted_durations AS (
    SELECT title, 
           REPLACE(duration, ' min', '') AS duration_numeric
    FROM netflix_titles
    WHERE show_type = 'Movie'
)
SELECT title, 
       duration_numeric
FROM formatted_durations
ORDER BY duration_numeric::INTEGER DESC
LIMIT 10;

--List the shows added to Netflix in 2021:
SELECT title, date_added
FROM netflix_titles
WHERE date_added LIKE '%2021%';

--Find the most common genres listed in Netflix titles:
SELECT listed_in, COUNT(*) AS genre_count
FROM netflix_titles
GROUP BY listed_in
ORDER BY genre_count DESC
LIMIT 10;

--List shows released in the last 5 years with their ratings:
SELECT title, release_year, rating
FROM netflix_titles
WHERE release_year >= (YEAR(CURRENT_DATE) - 5);

