from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['yelp_dataset']
review_collection = db["review"]

pipeline = [
    {
        "$project": {
            "words": {
                "$filter": {
                    "input": {"$split": [{"$toLower": "$text"}, " "]},
                    "as": "word",
                    "cond": {"$not": {"$in": ["$$word", ["the", "is", "and", "a", "to", "of"]]}}
                }
            }
        }
    },
    {"$unwind": "$words"},
    {
        "$group": {
            "_id": "$words",
            "count": {"$sum": 1}
        }
    },
    {"$sort": {"count": -1}},
    {"$limit": 5}
]

result = review_collection.aggregate(pipeline)

print("Top 5 Most Frequent Words (Excluding Common Stop Words):")
for doc in result:
    print(f"Word: {doc['_id']}, Count: {doc['count']}")
