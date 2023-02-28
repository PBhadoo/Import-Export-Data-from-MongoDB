import pymongo
import json
from datetime import datetime

print("Starting")
# Set up MongoDB connection
mongo_uri = "mongodb+srv://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
mongo_db_name = "xxxxxxxxx"
mongo_collection_name = "xxxxxxxxxxxxx"

# Connect to MongoDB and retrieve data
mongo_client = pymongo.MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db_name]
mongo_collection = mongo_db[mongo_collection_name]
print("Database Connected")
# Define filter query to select documents starting from _id=5000
filter_query = {"_id": {"$gte": 1}}
#filter_query = {"_id": {"$gte": 1, "$lte": 10000}}

# Retrieve data that matches the filter query
data = list(mongo_collection.find(filter_query))

def custom_json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")

print("Starting JSON dump...")

# Write the data to a .json file with each entry on a separate line
with open('output.json', 'w') as f:
    for entry in data:
        json.dump(entry, f, default=custom_json_serializer)
        f.write('\n')
print("JSON dump complete.")
