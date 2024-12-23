import pandas as pd
from textblob import TextBlob
from database_model import createTable, insertData


def getSentimentScore(comment):
    return TextBlob(str(comment)).sentiment.polarity


def extract(source):
    if source == 'sourceA':
        df = pd.read_csv('sourceA/reviews.csv')
        return df

    elif source == 'sourceB':
        df = pd.read_json('sourceB/reviews.json')
        return df

    elif source == 'sourceC':
        df = pd.DataFrame(columns=["review_id","product_id","customer_id","rating","review_date", "review_text"])

        with open("sourceC/reviews.txt", "r") as file:
            content = file.readlines()
        
        for review in content:
            row = review.split("|")
            row[5] = str(row[5][:-1])
            df = df._append(pd.Series(row, index=df.columns), ignore_index=True)

        return df


def transform(df1, df2, df3):
    df = pd.concat([df1, df2, df3], ignore_index=True)
    df = df[["review_id", "product_id", "customer_id", "rating", "review_date", "review_text"]]

    df = df.drop_duplicates(subset=['review_id'])
    df = df.dropna(subset=['rating'])

    df['review_date'] = pd.to_datetime(df['review_date']).dt.strftime('%Y-%m-%d')

    df['sentiment_score'] = df['review_text'].apply(getSentimentScore)

    return df


def load(df):
    createTable()
    insertData(df)



def main():
    df1 = extract('sourceA')
    df2 = extract('sourceB')
    df3 = extract('sourceC')

    df = transform(df1, df2, df3)

    load(df)


main()