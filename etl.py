import pandas as pd
import requests
from textblob import TextBlob
from database_model import insertData
from datetime import datetime



def extract():
    # Extracting data from CSV
    df_csv = pd.read_csv('sourceA/reviews.csv')

    # Extracting data from json with mock api
    url = "http://127.0.0.1:5000/api/reviews"
    try:
        response = requests.get(url)
        data = response.json()
        df_json = pd.DataFrame(data)
    except Exception as e:
        df_json = pd.DataFrame()

    # Extracting data from text file
    df_text = pd.DataFrame(columns=["review_id","product_id","customer_id","rating","review_date", "review_text"])

    with open("sourceC/reviews.txt", "r") as file:
        content = file.readlines()
    for review in content:
        row = review.split("|")
        row[5] = str(row[5][:-1])
        df_text = df_text._append(pd.Series(row, index=df_text.columns), ignore_index=True)
    
    return df_csv, df_json, df_text


def transform(df1, df2, df3):
    df = pd.concat([df1, df2, df3], ignore_index=True)
    df = df[["review_id", "product_id", "customer_id", "rating", "review_date", "review_text"]]

    df['review_id'] = pd.to_numeric(df['review_id'], errors='coerce')
    df['product_id'] = pd.to_numeric(df['product_id'], errors='coerce')
    df['customer_id'] = pd.to_numeric(df['customer_id'], errors='coerce')
    df['rating'] = pd.to_numeric(df['rating'], errors='coerce')
    df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce').dt.strftime('%Y-%m-%d')
    df['review_text'] = df['review_text'].fillna("")

    df.dropna(subset=['review_id', 'product_id', 'customer_id', 'rating', 'review_date'], inplace=True)
    
    df = df[df['rating'].between(1, 5)]

    df = df.drop_duplicates(subset=['review_id'])
    

    def getSentimentScore(comment):
        try:
            return TextBlob(str(comment)).sentiment.polarity
        except Exception as e:
            return 0
    
    df['sentiment_score'] = df['review_text'].apply(getSentimentScore)

    return df


def load(df):
    insertData(df)


def log(msg):
    time = datetime.now()
    timestamp = time.strftime("%d-%h-%Y %H:%M")

    with open("logfile.txt", "a") as file:
        file.write(timestamp + " -- " + msg + "\n")



if __name__ == "__main__":
    log("ETL job started.")

    log("Extract phase started.")
    df1, df2, df3 = extract()
    log("Extract phase completed.")

    log("Transform phase started.")
    df = transform(df1, df2, df3)
    log("Transform phase completed.")

    log("Load phase started.")
    load(df)
    log("Load phase completed.")

    log("ETL job completed successfully.")