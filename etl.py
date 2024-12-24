import pandas as pd
from textblob import TextBlob
from database_model import createTable, insertData



def extract():
    df_csv = pd.read_csv('sourceA/reviews.csv')

    df_json = pd.read_json('sourceB/reviews.json')

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
    createTable()
    insertData(df)



def main():
    df1, df2, df3 = extract()

    df = transform(df1, df2, df3)

    load(df)


main()