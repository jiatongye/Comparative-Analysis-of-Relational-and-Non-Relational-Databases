import json
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")  # Update the URI if hosted remotely
db = client['yelp_dataset']  # Create/use the database

# Function to insert data with only the necessary columns
def insert_data(file_path, collection_name, fields):
    collection = db[collection_name]  # Create/use the collection
    with open(file_path, 'r') as file:
        for line in file:
            document = json.loads(line)
            filtered_document = {field: document.get(field) for field in fields}
            collection.insert_one(filtered_document)
    print(f"Inserted filtered data from {file_path} into {collection_name} collection.")

# Define the required fields for each collection

# Business: Business_id, Name, Address, City, State, Stars, Review_count, Attributes, Categories, Hours
business_fields = [
    'business_id', 'name', 'address', 'city', 'state', 'stars', 'review_count', 'attributes', 'categories', 'hours'
]

# Review: Review_id, User_id, Business_id, Text, Stars, Date
review_fields = [
    'review_id', 'user_id', 'business_id', 'text', 'stars', 'date'
]

# User_info: User_id, Review_count, Yelping_since, Average_stars
user_info_fields = [
    'user_id', 'review_count', 'yelping_since', 'average_stars'
]

# Tip: Text, Date, Compliment_count, Business_id, User_id
tip_fields = [
    'text', 'date', 'compliment_count', 'business_id', 'user_id'
]

# Insert each dataset with only the required columns
insert_data('/Users/kellyhe/Desktop/data_101_finalproj/data/yelp_dataset/yelp_academic_dataset_business.json', 'business', business_fields)
insert_data('/Users/kellyhe/Desktop/data_101_finalproj/data/yelp_dataset/yelp_academic_dataset_review.json', 'review', review_fields)
insert_data('/Users/kellyhe/Desktop/data_101_finalproj/data/yelp_dataset/yelp_academic_dataset_user.json', 'user_info', user_info_fields)
insert_data('/Users/kellyhe/Desktop/data_101_finalproj/data/yelp_dataset/yelp_academic_dataset_tip.json', 'tip', tip_fields)
