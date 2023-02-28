import pymongo
import json

# This Script is not Tested. Try at your own risk.

# Set up MongoDB connection
mongo_uri = "mongodb+srv://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
mongo_db_name = "xxxxxxxxx"
mongo_collection_name = "xxxxxxxxxxxxx"

# Connect to MongoDB and retrieve data
mongo_client = pymongo.MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db_name]
mongo_collection = mongo_db[mongo_collection_name]

# Read data from output.json file
with open('output.json', 'r') as f:
    data = [json.loads(line) for line in f]

# Insert data into MongoDB collection in chunks of 5000 documents
chunk_size = 5000
num_documents = len(data)
for i in range(0, num_documents, chunk_size):
    chunk = data[i:i+chunk_size]
    result = mongo_collection.insert_many(chunk)
    print(f"Inserted {len(result.inserted_ids)} documents into {mongo_collection_name} collection.")
