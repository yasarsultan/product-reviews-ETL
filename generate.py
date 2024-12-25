import pandas as pd
import json
import random
from datetime import datetime, timedelta

def getReviewText(rating):
    comments = ["", "Terrible", "Poor", "Average", "Good", "Excellent"]
    return comments[rating]

def getRandomDate(date):
    random_num_days = random.randint(0, 345)
    return date + timedelta(days=random_num_days)

num_records = 2200
start_date = datetime(2023, 12, 30)

mocked_data = {
    "review_id": range(1, num_records+1),
    "product_id": [random.randint(100, 200) for _ in range(num_records)],
    "customer_id": [random.randint(1000, 2000) for _ in range(num_records)],
    "rating": [random.randint(1, 5) for _ in range(num_records)],
    "review_date": [str(getRandomDate(start_date).strftime("%Y-%m-%d")) for _ in range(num_records)]
}

mocked_data["review_text"] = [getReviewText(rating) for rating in mocked_data["rating"]]


csv_data = pd.DataFrame({key: mocked_data[key][:1000] for key in mocked_data})

json_data = [
    {key: mocked_data[key][i] for key in mocked_data}
    for i in range(1000, 1500)
]

text_data = [
    f"{mocked_data['review_id'][i]}|{mocked_data['product_id'][i]}|"
    f"{mocked_data['customer_id'][i]}|{mocked_data['rating'][i]}|"
    f"{mocked_data['review_date'][i]}|{mocked_data['review_text'][i]}"
    for i in range(1500, 2200)
]


# Saving Mocked Data
csv_data.to_csv('sourceA/reviews.csv', index=False)

with open("sourceB/reviews.json", "w") as file:
    json.dump(json_data, file, indent=4)

with open("sourceC/reviews.txt", "w") as file:
    file.write("\n".join(text_data))