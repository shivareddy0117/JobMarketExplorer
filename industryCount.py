from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('mongodb://localhost:27017/')
db = client['job_postings_db']

# Select or create a collection within the database
collection = db['job_postings'] # Use your actual collection name

# Aggregation pipeline
pipeline = [
    {
        "$group": {
            "_id": "$industry",  # Group by industry
            "companies": {"$addToSet": "$company_name"}  # Collect unique company names
        }
    },
    {
        "$sort": {"_id": 1}  # Optionally sort by industry alphabetically
    }
]

# Execute the aggregation query
results = collection.aggregate(pipeline)

# Print the results
for result in results:
    print(result)
