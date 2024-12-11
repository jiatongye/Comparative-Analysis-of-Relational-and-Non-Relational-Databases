from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['yelp_dataset']

review = db["review"]
result = review.aggregate([
    {"$limit": 1000},
    {
        "$lookup": {
            "from": "business",
            "localField": "business_id",
            "foreignField": "business_id",
            "as": "business_info"
        }
    },
    {"$unwind": "$business_info"},
    {
        "$group": {
            "_id": {"user_id": "$user_id", "state": "$business_info.state"},
            "avg_star": {"$avg": "$stars"}
        }
    },
    {
        "$group": {
            "_id": "$_id.user_id",
            "states": {"$addToSet": "$_id.state"},
            "overall_avg_rating": {"$avg": "$avg_star"}
        }
    },
    {
        "$match": {
            "$expr": {
                "$and": [
                    {"$gte": [{"$size": "$states"}, 3]},
                    {"$gt": ["$overall_avg_rating", 3.5]}
                ]
            }
        }
    },
    {
        "$lookup": {
            "from": "user",
            "localField": "_id",
            "foreignField": "user_id",
            "as": "user_info"
        }
    },
    {
        "$project": {
            "_id": 0,
            "user_id": "$_id",
            "state_count": {"$size": "$states"},
            "overall_avg_rating": 1,
            "user_info": {"name": 1}
        }
    }
])

for doc in result:
    print(doc)

client.close()
