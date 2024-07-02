from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['job_postings_db']

# Select or create a collection within the database
collection = db['job_postings']# Use your actual collection name

# Aggregation pipeline to categorize keywords by industry
pipeline = [
    # Optional: Uncomment the next line to filter by a specific industry
    # {"$match": {"industry": "Semiconductor Manufacturing"}},
    {"$unwind": "$keywords"},  # Unwind the array of keywords
    {"$group": {
        "_id": "$industry",  # Group by industry
        "unique_keywords": {"$addToSet": "$keywords"}  # Collect unique keywords
    }},
    {"$sort": {"_id": 1}}  # Optionally sort by industry alphabetically
]

# Execute the aggregation query
results = collection.aggregate(pipeline)

# Print the results
for result in results:
    print(f"Industry: {result['_id']}")
    print("Keywords:", result['unique_keywords'])
    print()
