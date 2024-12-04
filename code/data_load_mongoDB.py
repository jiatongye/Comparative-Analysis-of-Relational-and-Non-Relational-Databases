import json
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['yelp_dataset']

def insert_data(file_path, collection_name, fields):
    collection = db[collection_name]
    with open(file_path, 'r') as file:
        for line in file:
            document = json.loads(line)
            filtered_document = {field: document.get(field) for field in fields}
            collection.insert_one(filtered_document)
    print(f"Inserted filtered data from {file_path} into {collection_name} collection.")


business_fields = [
    'business_id', 'name', 'address', 'city', 'state', 'stars', 'review_count', 'attributes', 'categories', 'hours'
]

review_fields = [
    'review_id', 'user_id', 'business_id', 'text', 'stars', 'date', 'useful'
]

user_info_fields = [
    'user_id', 'review_count', 'yelping_since', 'average_stars'
]


insert_data('/Users/kellyhe/Desktop/data_101_finalproj/data/yelp_dataset/yelp_academic_dataset_business.json', 'business', business_fields)
insert_data('/Users/kellyhe/Desktop/data_101_finalproj/data/yelp_dataset/yelp_academic_dataset_review.json', 'review', review_fields)
insert_data('/Users/kellyhe/Desktop/data_101_finalproj/data/yelp_dataset/yelp_academic_dataset_user.json', 'user_info', user_info_fields)
