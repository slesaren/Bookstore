import json
import logging
from datetime import datetime
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)


def get_views_key(book_id):
    return f"book:{book_id}:views"


def get_rating_key(book_id):
    return f"book:{book_id}:avg_rating"


class RedisBookStats:

    def __init__(self, redis_client):
        self.redis = redis_client

    def increment_views(self, book_id):

        if self.redis is None:
            return False

        try:
            views_key = get_views_key(book_id)
            views = self.redis.incr(views_key)

            self.redis.zadd('popular:books', {book_id: views})

            return views
        except RedisError as e:
            logger.error(f"Redis error incrementing views for book {book_id}: {e}")
            return False



    def get_views(self, book_id):
        if self.redis is None:
            return 0

        try:
            views_key = get_views_key(book_id)
            views = self.redis.get(views_key)
            result = int(views) if views else 0
            logger.debug(f"Getting views for book {book_id}: {result}")
            return result
        except RedisError as e:
            logger.error(f"Redis error getting views for book {book_id}: {e}")
            return 0


    def get_popular_books(self, limit=10):
        if not self.redis:
            return []

        try:
            top_books = self.redis.zrange('popular:books', 0, limit - 1, withscores=True)

            result = []
            for book_id_str, views in top_books:
                book_id = int(book_id_str)
                result.append({
                    'id': book_id,
                    'views': int(views)
                })

            return result
        except RedisError as e:
            logger.error(f"Redis error getting popular books: {e}")
            return []

    def cache_rating(self, book_id, rating, ttl=3600):
        if not self.redis:
            return False

        try:
            key = get_rating_key(book_id)
            self.redis.setex(key, ttl, rating)
            return True
        except RedisError as e:
            logger.error(f"Redis error caching rating for book {book_id}: {e}")
            return False

    def get_cached_rating(self, book_id):
        if not self.redis:
            return None

        try:
            key = get_rating_key(book_id)
            rating = self.redis.get(key)
            if rating is not None:
                return float(rating)
            return None
        except RedisError as e:
            logger.error(f"Redis error getting cached rating for book {book_id}: {e}")
            return None

    def delete_cached_rating(self, book_id):
        if not self.redis:
            return False

        try:
            key = get_rating_key(book_id)
            self.redis.delete(key)
            return True
        except RedisError as e:
            logger.error(f"Redis error deleting cached rating for book {book_id}: {e}")
            return False