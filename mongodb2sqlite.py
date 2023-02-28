import pymongo
import pandas as pd
from sqlalchemy import create_engine

# Set up MongoDB connection
mongo_uri = "mongodb+srv://xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
mongo_db_name = "xxxxxxxxx"
mongo_collection_name = "xxxxxxxxxxxxx"

# Set up SQLite connection
sqlite_file_name = "output.sqlite3"

# Connect to MongoDB and retrieve data
mongo_client = pymongo.MongoClient(mongo_uri)
mongo_db = mongo_client[mongo_db_name]
mongo_collection = mongo_db[mongo_collection_name]

# Define filter query to select documents starting from _id=5000
filter_query = {"_id": {"$gte": 1}}

# Retrieve data that matches the filter query
data = list(mongo_collection.find(filter_query))

# Convert data to a Pandas DataFrame
df = pd.DataFrame(data)

# Write the DataFrame to a SQLite database
engine = create_engine(f"sqlite:///{sqlite_file_name}")
df.to_sql(mongo_collection_name, engine, if_exists="replace", index=False)
