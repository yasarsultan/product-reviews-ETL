import os
from psycopg2 import connect, sql
from dotenv import load_dotenv

load_dotenv()

def getConnection():
    connection = connect(
        dbname = os.getenv('DB_NAME'),
        user = os.getenv('DB_USER'),
        password = os.getenv('DB_PASS'),
        host = "localhost",
        port = "5432"
    )
    connection.autocommit = True

    return connection


def createTable(cursor):
    cursor.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS reviews(
                review_id NUMERIC PRIMARY KEY,
                product_id NUMERIC NOT NULL,
                customer_id NUMERIC NOT NULL,
                rating DECIMAL NOT NULL,
                review_date DATE NOT NULL,
                review_text TEXT,
                sentiment_score DECIMAL)
            """
        )
    )


def insertData(df):
    connection = getConnection()
    cursor = connection.cursor()

    createTable(cursor)

    for index, row in df.iterrows():
        cursor.execute(
            sql.SQL(
                """
                INSERT INTO reviews(review_id, product_id, customer_id, rating, review_date, review_text, sentiment_score)
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (review_id) DO NOTHING;
                """
            ),
            (row['review_id'], row['product_id'], row['customer_id'], row['rating'], row['review_date'], row['review_text'], row['sentiment_score'])
        )
        
    cursor.close()
    connection.close()
