from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['yelp_dataset']
business = db['business']

pipeline = [    
    {"$addFields": {
        "isTakeOut": {
            "$cond": {
                "if": {"$eq": ["$attributes.RestaurantsTakeOut", "True"]},
                "then": 1,
                "else": 0
            }
        }
    }},
    
    {"$group": {
        "_id": "$state",
        "takeOutCount": {"$sum": "$isTakeOut"},
        "totalCount": {"$sum": 1}
    }},
    
    {"$addFields": {
        "takeOutProportion": {"$divide": ["$takeOutCount", "$totalCount"]}
    }},
    
    {"$sort": {"takeOutProportion": -1}},
    
    {"$limit": 5}
]


print("Top 5 States by Takeout Proportion:")
results = db.business.aggregate(pipeline)
for result in results:
    print(result)

print(db.command('explain', {'aggregate': 'business', 'pipeline': pipeline, 'cursor': {}}, verbosity='executionStats'))
