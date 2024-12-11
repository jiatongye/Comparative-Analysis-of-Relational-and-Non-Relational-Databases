from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client['yelp_dataset']
review = db['review']
business = db['business']

pipeline = [
{"$limit": 1000},
  {
        "$lookup": {
        "from": "business",
        "localField": "business_id",
        "foreignField": "business_id",
        "as": "business_info"
	}
  },
  {
	"$unwind": "$business_info"
  },
  {
	"$addFields": {
  	"state": "$business_info.state"
	}
  },
  {
	"$lookup": {
  	"from": "user_info",
  	"localField": "user_id",
  	"foreignField": "user_id",
  	"as": "user_info"
	}
  },
  {
	"$unwind": "$user_info"
  },
  {
	"$group": {
  	"_id": { "user_id": "$user_id", "state": "$state" },
  	"total_useful": { "$sum": "$useful" },
  	"total_reviews": { "$sum": 1 },
  	"average_stars": { "$first": "$user_info.average_stars" },
  	"yelping_since": { "$first": "$user_info.yelping_since" }
	}
  },
  {
	"$addFields": {
  	"avg_helpful_votes": { "$divide": ["$total_useful", "$total_reviews"] }
	}
  },
  {
	"$group": {
  	"_id": "$_id.state",
  	"top_users": {
    	"$push": {
      	"user_id": "$_id.user_id",
      	"avg_helpful_votes": "$avg_helpful_votes",
      	"total_reviews": "$total_reviews",
      	"total_useful": "$total_useful",
      	"average_stars": "$average_stars",
      	"yelping_since": "$yelping_since"
    	}
  	}
	}
  },
  {
	"$project": {
  	"top_users": { "$slice": [{ "$sortArray": { "input": "$top_users", "sortBy": { "avg_helpful_votes": -1 } } }, 5] }
	}
  }
]

results = db.review.aggregate(pipeline)

print("Top 5 users with the highest average helpful votes per review for each state:")
for result in results:
    print(result)

print(db.command('explain', {'aggregate': 'review', 'pipeline': pipeline, 'cursor': {}}, verbosity='executionStats'))

