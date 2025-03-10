# product-reviews-ETL


## Introduction

This project implements an ETL (Extract, Transform, Load) pipeline to process product review data from multiple sources, including structured CSV file, JSON api and unstructured text file. The pipeline integrates data, cleans and normalizes it into a unified schema, further adds sentiment analysis to it, and finally stores the processed information in a PostgreSQL database. This allows organizations to derive actionable insights from the data.



## Setup Instructions

1. Clone this repository:
```bash
git clone https://github.com/yasarsultan/product-reviews-ETL
cd product-reviews-ETL
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Set environment variables:
```bash
touch .env
```
Then inside .env file add some variables like `DB_NAME`  `DB_USER` `DB_PASS`

4. Run ETL pipeline:
```bash
python3 app.py
```

```bash
python3 etl.py
```

## SQL Queries
1. Average product ratings:
```sql
SELECT product_id, AVG(rating) FROM reviews
GROUP BY product_id
ORDER BY product_id;
```
![img](https://github.com/yasarsultan/product-reviews-ETL/blob/main/images/avg_ratings.png)
![img0](https://github.com/yasarsultan/product-reviews-ETL/blob/main/images/avg_ratings_query.png)

2. Top 3 highest rated products:
```sql
SELECT product_id, AVG(rating) FROM reviews
GROUP BY product_id
ORDER BY AVG(rating) DESC
LIMIT 3;
```
![img1](https://github.com/yasarsultan/product-reviews-ETL/blob/main/images/highest_rated.png)

3. Top 3 lowest rated products:
```sql
SELECT product_id, AVG(rating) FROM reviews
GROUP BY product_id
ORDER BY AVG(rating) ASC
LIMIT 3;
```
![img2](https://github.com/yasarsultan/product-reviews-ETL/blob/main/images/lowest_rated.png)

4. Sample data in database:
![img3](https://github.com/yasarsultan/product-reviews-ETL/blob/main/images/sample_db.png)
