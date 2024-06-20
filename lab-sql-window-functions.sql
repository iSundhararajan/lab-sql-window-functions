-- Challenge 1
-- This challenge consists of three exercises that will test your ability to use the SQL RANK() function. 
-- You will use it to rank films by their length, their length within the rating category, 
-- and by the actor or actress who has acted in the greatest number of films.

--     Rank films by their length and create an output table that includes the title, length, and rank columns only. 
--     Filter out any rows with null or zero values in the length column.
USE sakila;
-- Create the output table
CREATE TABLE ranked_films (
    title VARCHAR(255),
    length INT,
    ranked INT
);

-- Insert data into the output table with ranking
INSERT INTO ranked_films (title, length, ranked)
SELECT 
    title,
    length,
    DENSE_RANK() OVER (ORDER BY length DESC) AS ranked
FROM 
    film
WHERE 
    length IS NOT NULL AND length > 0;

-- View the results
SELECT * FROM ranked_films;

--     Rank films by length within the rating category and create an output table that includes the title, length, 
--     rating and rank columns only. Filter out any rows with null or zero values in the length column.
-- Create the output table
CREATE TABLE ranked_films_by_rating (
    title VARCHAR(255),
    length INT,
    rating VARCHAR(10),
    ranked_f INT
);

-- Insert data into the output table with ranking within rating category
INSERT INTO ranked_films_by_rating (title, length, rating, ranked_f)
SELECT 
    title,
    length,
    rating,
    DENSE_RANK() OVER (PARTITION BY rating ORDER BY length DESC) AS ranked_f
FROM 
    film
WHERE 
    length IS NOT NULL AND length > 0;

-- View the results
SELECT * FROM ranked_films_by_rating;

--     Produce a list that shows for each film in the Sakila database, 
--     the actor or actress who has acted in the greatest number of films, as well as the total number of films in which they have acted. 
--     Hint: Use temporary tables, CTEs, or Views when appropiate to simplify your queries.
-- Step 1: Create a CTE to calculate the number of films each actor has acted in
WITH actor_film_count AS (
    SELECT 
        a.actor_id,
        a.first_name,
        a.last_name,
        COUNT(fa.film_id) AS total_films
    FROM 
        actor a
    JOIN 
        film_actor fa ON a.actor_id = fa.actor_id
    GROUP BY 
        a.actor_id, a.first_name, a.last_name
),

-- Step 2: Create another CTE to join the films with actors and include the total films count for each actor
film_actor_details AS (
    SELECT 
        f.film_id,
        f.title,
        a.actor_id,
        a.first_name,
        a.last_name,
        afc.total_films
    FROM 
        film f
    JOIN 
        film_actor fa ON f.film_id = fa.film_id
    JOIN 
        actor_film_count afc ON fa.actor_id = afc.actor_id
    JOIN 
        actor a ON a.actor_id = fa.actor_id
),

-- Step 3: Create a CTE to rank actors within each film by their total number of films acted in
ranked_actors AS (
    SELECT 
        fad.film_id,
        fad.title,
        fad.actor_id,
        fad.first_name,
        fad.last_name,
        fad.total_films,
        DENSE_RANK() OVER (PARTITION BY fad.film_id ORDER BY fad.total_films DESC) AS ranked
    FROM 
        film_actor_details fad
)

-- Step 4: Select the top-ranked actor for each film
SELECT 
    ra.title,
    ra.first_name,
    ra.last_name,
    ra.total_films
FROM 
    ranked_actors ra
WHERE 
    ra.ranked = 1
ORDER BY 
    ra.title;

-- Challenge 2

-- This challenge involves analyzing customer activity and retention in the Sakila database to gain insight into business performance. By analyzing customer behavior over time, 
-- businesses can identify trends and make data-driven decisions to improve customer retention and increase revenue.
-- The goal of this exercise is to perform a comprehensive analysis of customer activity and retention by conducting an analysis on the monthly percentage 
-- change in the number of active customers and the number of retained customers. 
-- Use the Sakila database and progressively build queries to achieve the desired outcome.

--     Step 1. Retrieve the number of monthly active customers, i.e., the number of unique customers who rented a movie in each month.
-- Step 1: Retrieve the number of monthly active customers using subqueries and window functions
WITH monthly_customer_counts AS (
    SELECT
        DATE_FORMAT(r.rental_date, '%Y-%m') AS month,
        r.customer_id
    FROM
        rental r
    GROUP BY
        DATE_FORMAT(r.rental_date, '%Y-%m'),
        r.customer_id
),
monthly_active_customers AS (
    SELECT
        month,
        COUNT(customer_id) AS active_customers
    FROM
        monthly_customer_counts
    GROUP BY
        month
)
SELECT
    month,
    active_customers
FROM
    monthly_active_customers
ORDER BY
    month;

--     Step 2. Retrieve the number of active users in the previous month.
-- Step 2: Retrieve the number of active users in the previous month using window functions
WITH monthly_active_customers AS (
    SELECT
        DATE_FORMAT(r.rental_date, '%Y-%m') AS month,
        COUNT(DISTINCT r.customer_id) AS active_customers
    FROM
        rental r
    GROUP BY
        DATE_FORMAT(r.rental_date, '%Y-%m')
),
active_customers_with_lag AS (
    SELECT
        month,
        active_customers,
        LAG(active_customers) OVER (ORDER BY month) AS previous_month_active_customers
    FROM
        monthly_active_customers
)
SELECT
    month,
    active_customers,
    previous_month_active_customers
FROM
    active_customers_with_lag
ORDER BY
    month;

--     Step 3. Calculate the percentage change in the number of active customers between the current and previous month.

WITH monthly_active_customers AS (
    SELECT
        DATE_FORMAT(r.rental_date, '%Y-%m') AS month,
        COUNT(DISTINCT r.customer_id) AS active_customers
    FROM
        rental r
    GROUP BY
        DATE_FORMAT(r.rental_date, '%Y-%m')
),
active_customers_with_lag AS (
    SELECT
        month,
        active_customers,
        LAG(active_customers) OVER (ORDER BY month) AS previous_month_active_customers
    FROM
        monthly_active_customers
),
percentage_change AS (
    SELECT
        month,
        active_customers,
        previous_month_active_customers,
        ROUND(
            (active_customers - previous_month_active_customers) / previous_month_active_customers * 100, 
            2
        ) AS percentage_change
    FROM
        active_customers_with_lag
)
SELECT
    month,
    active_customers,
    previous_month_active_customers,
    percentage_change
FROM
    percentage_change
ORDER BY
    month;

--     Step 4. Calculate the number of retained customers every month, i.e., customers who rented movies in the current and previous months.
-- Step 4: Calculate the number of retained customers every month
WITH monthly_active_customers AS (
    SELECT
        DATE_FORMAT(r.rental_date, '%Y-%m') AS month,
        COUNT(DISTINCT r.customer_id) AS active_customers
    FROM
        rental r
    GROUP BY
        DATE_FORMAT(r.rental_date, '%Y-%m')
),
active_customers_with_lag AS (
    SELECT
        month,
        active_customers,
        LAG(active_customers) OVER (ORDER BY month) AS previous_month_active_customers
    FROM
        monthly_active_customers
),
retained_customers AS (
    SELECT
        month,
        active_customers,
        COALESCE(previous_month_active_customers, 0) AS previous_month_active_customers,
        active_customers - COALESCE(previous_month_active_customers, 0) AS retained_customers
    FROM
        active_customers_with_lag
)
SELECT
    month,
    retained_customers
FROM
    retained_customers
ORDER BY
    month;
-- Hint: Use temporary tables, CTEs, or Views when appropiate to simplify your queries.