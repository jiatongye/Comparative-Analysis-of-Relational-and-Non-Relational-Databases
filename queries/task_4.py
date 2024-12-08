from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['yelp_dataset']
business = db['business']

result_business_counts = business.aggregate([
    {
        "$group": {
            "_id": "$state",  
            "total_businesses": { "$sum": 1 }  # Count total businesses per state
        }
    },
    {
        "$merge": { "into": "BusinessCounts" }  # Create a new collection for BusinessCounts
    }
])


result_takeout_counts = business.aggregate([
    {
        "$match": { "attributes.RestaurantsTakeOut": "True" }  # Filter by restaurants that offer takeout
    },
    {
        "$group": {
            "_id": "$state", 
            "takeout_businesses": { "$sum": 1 }  # Count takeout businesses per state
        }
    },
    {
        "$merge": { "into": "TakeOutCounts" }  # Create a new collection for TakeOutCounts
    }
])


results = db.BusinessCounts.aggregate([
    {
        "$lookup": {
            "from": "TakeOutCounts",  # Perform a join with TakeOutCounts
            "localField": "_id",  # Join by 'state'
            "foreignField": "_id",
            "as": "takeout_counts"
        }
    },
    {
        "$unwind": "$takeout_counts"  # Flatten the array of matched results
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
            "_id": 0,  # Exclude the '_id' field
            "state": "$_id",  # Include the 'state' field
            "takeout_proportion": 1  # Include the 'takeout_proportion' field
        }
    }
])


print("Top 5 States by Takeout Proportion:")
for result in results:
    print(result)

