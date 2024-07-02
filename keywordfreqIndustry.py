from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['job_postings_db']

# Select or create a collection within the database
collection = db['job_postings']# Use your actual collection name

# Aggregation pipeline for keyword frequency by industry
pipeline = [
    {"$unwind": "$keywords"},  # Unwind the keywords array
    {"$group": {
        "_id": {
            "industry": "$industry",  # Group by industry
            "keyword": "$keywords"  # And by keyword
        },
        "count": {"$sum": 1}  # Count occurrences
    }},
    {"$sort": {"_id.industry": 1, "count": -1}},  # Sort by industry, and then by count descending
    {"$group": {  # Group to push keywords back into arrays
        "_id": "$_id.industry",
        "keywords": {
            "$push": {
                "keyword": "$_id.keyword",
                "count": "$count"
            }
        }
    }},
    {"$sort": {"_id": 1}}  # Sort by industry name
]

# Execute the aggregation query
results = collection.aggregate(pipeline)

# Print the results
for result in results:
    print(f"Industry: {result['_id']}")
    for keyword_info in result['keywords']:
        print(f"{keyword_info['keyword']}: {keyword_info['count']}")
    print()
