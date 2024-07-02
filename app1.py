
from pymongo import MongoClient

import json
import re
import os

from groq import Groq


# Connect to the MongoDB server running on localhost:27017
mongo_client = MongoClient('mongodb://localhost:27017/')

# Select or create a database
db = mongo_client['job_postings_db']
collection = db['job_postings']

llm_client = Groq(
    api_key="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"
)
jds = ["""""", """""",""""""]
for _ in jds:
        
    chat_completion = llm_client.chat.completions.create(
        messages=[
            {
                "role": "user",
                "content": _ + """job_details = {
        "job_id": "123456", "Job_name": "Machine Learning Engineer", 
        "company_name": "KLA", "industry": "Renewable Energy Equipment Manufacturing",
        "job_description": "Contribute to novel image processing, defect detection and analysis, optimization algorithms, and evaluate and productize deep learning approaches.",
        "keywords": ["deep learning", "image processing", "C++", "algorithm development"]
    }


    in this format extract the details from the above make sure the keywords should contain all to qulaify for the ATS, output only the Json of Job_details dont output anything else""",
            }
        ],
        model="llama3-8b-8192",
    )


    llm_output = chat_completion.choices[0].message.content
    # Regex to find content inside curly braces
    extracted_json = re.findall(r'\{([^}]*)\}', llm_output)
    if extracted_json:
        # Join the extracted JSON parts and wrap it around braces
        complete_json = "{" + extracted_json[0] + "}"
        try:
            # Convert the JSON string to a dictionary
            # job_details = json.loads(complete_json)
            # # Connection to MongoDB
            # client = MongoClient('mongodb://localhost:27017/')
            # db = client['your_database_name']
            # collection = db['your_collection_name']
            # # Insert the dictionary into MongoDB
            job_details = json.loads(complete_json)
            collection.insert_one(job_details)
            print("Job details inserted successfully.")
        except json.JSONDecodeError as e:
            print("JSON decode error:", e)
        except TypeError as e:
            print("Type error:", e)
    else:
        print("No valid JSON found.")