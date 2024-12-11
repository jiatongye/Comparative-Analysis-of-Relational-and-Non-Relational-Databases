from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['yelp_dataset']

db["review"].create_index([("business_id", 1), ("user_id", 1)])
db["business"].create_index([("business_id", 1), ("state", 1)])
db["user"].create_index([("user_id", 1)])

pipeline = [
    {"$limit": 1000},
    {
        "$lookup": {
            "from": "business",
            "let": {"business_id": "$business_id"},
            "pipeline": [
                {"$match": {"$expr": {"$eq": ["$business_id", "$$business_id"]}}},
                {"$project": {"state": 1}}
            ],
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
            "pipeline": [{"$project": {"name": 1}}],
            "as": "user_info"
        }
    },
    {
        "$project": {
            "_id": 0,
            "user_id": "$_id",
            "state_count": {"$size": "$states"},
            "overall_avg_rating": 1,
            "user_info": {"$arrayElemAt": ["$user_info.name", 0]}
        }
    }
]

result = db["review"].aggregate(pipeline)

for doc in result:
    print(doc)
