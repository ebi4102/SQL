DROP TABLE IF EXISTS retail;

CREATE TABLE retail
(
    transactions_id INT PRIMARY KEY,
    sale_date DATE,	
    sale_time TIME,
    customer_id INT,	
    gender VARCHAR(10),
    age INT,
    category VARCHAR(35),
    quantity INT,
    price_per_unit FLOAT,	
    cogs FLOAT,
    total_sale FLOAT
);

COPY retail FROM 'D:\postsql\re.csv' WITH CSV HEADER;

SELECT * FROM retail;

#DETERMINE THE TOTAL NUMBER OF RECORDS IN THE DATASET

SELECT count( * ) FROM retail;

#IDENTIFY UNIQUE PRODUCT CATEGORIES IN THE DATASET

SELECT DISTINCT category FROM retail;

#CHECK FOR NULL VALUES IN DATASET AND DELETE IF ANY

SELECT * FROM retail
WHERE transactions_id IS NULL OR sale_date IS NULL OR sale_time IS NULL OR customer_id IS NULL OR gender IS NULL OR age IS NULL OR category IS NULL OR
quantity IS NULL OR price_per_unit IS NULL OR cogs IS NULL OR total_sale IS NULL;

DELETE FROM retail
WHERE transactions_id IS NULL OR sale_date IS NULL OR sale_time IS NULL OR customer_id IS NULL OR gender IS NULL OR age IS NULL OR category IS NULL OR
quantity IS NULL OR price_per_unit IS NULL OR cogs IS NULL OR total_sale IS NULL;

#WRITE A QUERY TO RETRIEVE ALL COLUMNS FOR SALES MADE IN '2022-11-05'

SELECT * FROM retail
WHERE sale_date='2022-11-05';

#WRITE A QUERY TO RETRIEVE ALL TRANSACTIONS WHERE THE CATEGORY IS CLOTHING AND THE QUANTITY SOLD IS MORE THAN 4 IN THE MONTH OF NOVEMBER 2022

SELECT * 
FROM retail 
WHERE
    category='clothing' 
    AND
    to_char(sale_date,'YYYY-MM')='2022-11' 
    AND
    quantity>=4;
    
#WRITE A QUERY TO CALCULATE TOTAL SALES FOR EACH category

SELECT
    category,
    sum(total_sale) AS net_sale,
    count(*) AS total_orders
FROM retail
GROUP BY 1;

#WRITE A QUERY TO FIND AVERAGE AGE OF CUSTOMERS WHO PURCHASED ITEMS FROM BEAUTY category

SELECT
    ROUND(AVG(age),2) AS AVG_AGE
FROM retail
WHERE category='Beauty'; 

#WRITE A QUERY TO FIND ALL TRANSACTIONS WHERE TOTAL_SALE IS GREATER THAN 1500

SELECT * FROM retail
WHERE total_sale >1500;

#WRITE A QUERY TO FIND THE TOTAL NUMBER OF TRANSACTIONS (transaction_id) MADE BY EACH GENDER IN EACH CATEGORY

SELECT 
    category,
    gender,
    COUNT(*) AS TOTAL_TRANSACTIONS
FROM retail
GROUP BY 
    category,
    gender
ORDER BY
    1;
    
#WRITE A QUERY TO CALCULATE AVERAGE SALE FOR EACH MONTH. FIND OUT BEST SELLING MONTH IN EACH YEAR.

SELECT 
    YEAR,
    MONTH,
    avg_sale
FROM
(
SELECT 
    EXTRACT(YEAR FROM sale_date) AS YEAR,
    EXTRACT(MONTH FROM sale_date) AS MONTH,
    AVG(total_sale) AS avg_sale,
    RANK() OVER(PARTITION BY EXTRACT(YEAR FROM sale_date) ORDER BY avg(total_sale) DESC) AS rank
FROM retail
GROUP BY 1, 2
) AS t1
WHERE rank = 1

#WRITE A QUERY TO FIND TOP 5 CUSTOMERS BASED ON HIGHEST TOTAL SALES

SELECT
    customer_id,
    SUM(total_sale) AS Total_sales
FROM retail
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;   

#WRITE A QUERY TO FIND NUMBER OF UNIQUE CUSTOMERS WHO PURCHASED ITEMS FROM EACH CATEGORY.

SELECT 
    category,    
    COUNT(DISTINCT customer_id) AS cnt_unique_cs
FROM retail
GROUP BY category;

#WRITE A QUERY TO CREATE EACH SHIFT AND NUMBER OF ORDERS (Example Morning <12, Afternoon BETWEEN 12 & 17, Evening >17)

WITH hourly_sale
AS
(
SELECT *,
    CASE
        WHEN EXTRACT(HOUR FROM sale_time) < 12 THEN 'Morning'
        WHEN EXTRACT(HOUR FROM sale_time) BETWEEN 12 AND 17 THEN 'Afternoon'
        ELSE 'Evening'
    END AS shift
FROM retail
)
SELECT 
    shift,
    COUNT(*) AS total_orders    
FROM hourly_sale
GROUP BY shift;

