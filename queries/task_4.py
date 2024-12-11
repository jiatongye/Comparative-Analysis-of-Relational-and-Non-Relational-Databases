from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['yelp_dataset']
business = db['business']

businessCounts_pipeline = [
    {
        "$group": {
            "_id": "$state",  
            "total_businesses": { "$sum": 1 } 
        }
    },
    {
        "$merge": { "into": "BusinessCounts" }  
    }
]

business.aggregate(businessCounts_pipeline)

takoutCounts_pipeline = [
    {
        "$match": { "attributes.RestaurantsTakeOut": "True" }  
    },
    {
        "$group": {
            "_id": "$state", 
            "takeout_businesses": { "$sum": 1 } 
        }
    },
    {
        "$merge": { "into": "TakeOutCounts" }  
    }
]


business.aggregate(takoutCounts_pipeline)

proportion_pipeline = [
    {
        "$lookup": {
            "from": "TakeOutCounts",  
            "localField": "_id",  
            "foreignField": "_id",
            "as": "takeout_counts"
        }
    },
    {
        "$unwind": "$takeout_counts"  
    },
    {
        "$addFields": {
            "takeout_proportion": {
                "$divide": [
                    { "$multiply": ["$takeout_counts.takeout_businesses", 1.0] },
                    "$total_businesses"
                ]
            }
        }
    },
    {
        "$sort": { "takeout_proportion": -1 }  
    },
    {
        "$limit": 5 
    },
    {
        "$project": {
            "_id": 0, 
            "state": "$_id",  
            "takeout_proportion": 1 
        }
    }
]

results = db.BusinessCounts.aggregate(proportion_pipeline)


print("Top 5 States by Takeout Proportion:")
for result in results:
    print(result)

print(db.command('explain', {'aggregate': 'business', 'pipeline': proportion_pipeline, 'cursor': {}}, verbosity='executionStats'))
