import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
                              'postgresql://postgres:marslon@localhost:5432/bookstore?client_encoding=utf8'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'your-secret-key-here'

    MONGO_URI = os.environ.get('MONGO_URI') or 'mongodb://localhost:27017/'
    MONGO_DB = os.environ.get('MONGO_DB') or 'bookstore'

    REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
    REDIS_DB = int(os.environ.get('REDIS_DB', 0))
    REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
    REDIS_SOCKET_TIMEOUT = int(os.environ.get('REDIS_SOCKET_TIMEOUT', 5))


    RATING_CACHE_TTL = 3600
    VIEWS_CACHE_TTL = 86400
    POPULAR_CACHE_TTL = 300
