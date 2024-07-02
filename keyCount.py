from pymongo import MongoClient

# Connect to the MongoDB server (ensure your connection string is correct)

mongo_client = MongoClient('mongodb://localhost:27017/')

# Select or create a database
db = mongo_client['job_postings_db']
collection = db['job_postings']
# Aggregation pipeline
pipeline = [
    {"$unwind": "$keywords"},  # Unwind the array of keywords into separate documents
    {"$group": {
        "_id": "$keywords",  # Group by keywords
        "count": {"$sum": 1}  # Count the occurrences of each keyword
    }},
    {"$sort": {"count": -1}}  # Optional: sort the results by count in descending order
]

# Perform the aggregation
results = collection.aggregate(pipeline)

# Print the results
for result in results:
    print(result)
