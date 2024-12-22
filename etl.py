import pandas as pd


def extract(source):
    if source == 'sourceA':
        df = pd.read_csv('sourceA/reviews.csv')
        return df

    elif source == 'sourceB':
        df = pd.read_json('sourceB/reviews.json')
        return df

    elif source == 'sourceC':
        df = pd.DataFrame(columns=["review_id","product_id","customer_id","rating","review_date"])

        with open("sourceC/reviews.txt", "r") as file:
            content = file.readlines()
        
        for review in content:
            row = review.split("|")
            row[-1] = str(row[-1][1:-2])
            df = df._append(pd.Series(row, index=df.columns), ignore_index=True)

        return df

def transform():
    pass

def load():
    pass


def main():
    df1 = extract('sourceA')
    # print(df1.head())

    df2 = extract('sourceB')
    # print(df2.head())

    df3 = extract('sourceC')
    # print(df2.head())

    df = pd.concat([df1, df2, df3], ignore_index=True)

    print(df.head(10))

main()