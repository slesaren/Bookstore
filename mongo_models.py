from flask_sqlalchemy import SQLAlchemy
from datetime import datetime

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import CheckConstraint, UniqueConstraint
from datetime import datetime

from pymongo import MongoClient
from config import Config

client = MongoClient(Config.MONGO_URI)
mongo_db = client[Config.MONGO_DB]

reviews_collection = mongo_db['reviews']
promotions_collection = mongo_db['promotions']

reviews_collection.create_index('book_id')
promotions_collection.create_index('valid_until')