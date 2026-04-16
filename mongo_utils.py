import logging
from datetime import datetime
from bson import ObjectId
from pymongo.errors import PyMongoError

from mongo_models import promotions_collection

logger = logging.getLogger(__name__)


def get_book_rating_stats(reviews_collection, book_id):

    if not reviews_collection is None:
        return {'avg_rating': 0, 'review_count': 0}

    try:
        pipeline = [
            {'$match': {'book_id': book_id}},
            {'$group': {
                '_id': None,
                'avg_rating': {'$avg': '$rating'},
                'review_count': {'$sum': 1}
            }}
        ]

        result = list(reviews_collection.aggregate(pipeline))

        if result:
            return {
                'avg_rating': round(result[0]['avg_rating'], 1),
                'review_count': result[0]['review_count']
            }

        return {'avg_rating': 0, 'review_count': 0}

    except PyMongoError as e:
        logger.error(f"MongoDB error getting rating stats for book {book_id}: {e}")
        return {'avg_rating': 0, 'review_count': 0}


def get_popular_books_advanced(reviews_collection, order_history_model, db_session, redis_stats, limit=10,
                               min_reviews=3):

    if reviews_collection is None:
        logger.warning("reviews_collection is None")
        return []

    try:
        pipeline = [
            {'$match': {'rating': {'$exists': True}}},
            {'$group': {
                '_id': '$book_id',
                'avg_rating': {'$avg': '$rating'},
                'review_count': {'$sum': 1}
            }},
            {'$match': {'review_count': {'$gte': min_reviews}}}
        ]

        books_with_reviews = list(reviews_collection.aggregate(pipeline))

        if not books_with_reviews:
            logger.info(f"No books with at least {min_reviews} reviews found")
            return []

        rating_dict = {
            book['_id']: {
                'avg_rating': book['avg_rating'],
                'review_count': book['review_count']
            }
            for book in books_with_reviews
        }

        book_ids = list(rating_dict.keys())

        books = db_session.query(order_history_model).filter(
            order_history_model.id.in_(book_ids)
        ).all()

        MAX_VIEWS = 10000

        books_with_score = []
        for book in books:
            book_data = rating_dict.get(book.id, {})
            avg_rating = book_data.get('avg_rating', 0)
            review_count = book_data.get('review_count', 0)

            views = 0
            if redis_stats is not None:
                views = redis_stats.get_views(book.id)

            normalized_views = min(views / MAX_VIEWS, 1.0) * 100

            combined_score = (normalized_views * 0.3) + (avg_rating * 0.7)

            books_with_score.append({
                'id': book.id,
                'title': book.title,
                'author': book.author,
                'price': float(book.price),
                'rating': avg_rating,
                'views': views,
                'review_count': review_count,
                'score': combined_score
            })

        books_with_score.sort(key=lambda x: x['score'], reverse=True)

        return books_with_score[:limit]

    except PyMongoError as e:
        logger.error(f"MongoDB aggregation error: {e}")
        return []
    except Exception as e:
        logger.error(f"Error in get_popular_books_advanced: {e}")
        return []


def get_book_rating_stats(reviews_collection, book_id):

    if reviews_collection is None:
        return {'avg_rating': 0, 'review_count': 0}

    try:
        pipeline = [
            {'$match': {'book_id': book_id}},
            {'$group': {
                '_id': None,
                'avg_rating': {'$avg': '$rating'},
                'review_count': {'$sum': 1}
            }}
        ]

        result = list(reviews_collection.aggregate(pipeline))

        if result:
            return {
                'avg_rating': round(result[0]['avg_rating'], 1),
                'review_count': result[0]['review_count']
            }

        return {'avg_rating': 0, 'review_count': 0}

    except PyMongoError as e:
        logger.error(f"MongoDB error getting rating stats for book {book_id}: {e}")
        return {'avg_rating': 0, 'review_count': 0}


def update_book_rating(db_session, book_model, reviews_collection, book_id):
    if reviews_collection is None:
        logger.warning("MongoDB reviews collection not available")
        return False

    try:
        stats = get_book_rating_stats(reviews_collection, book_id)
        avg_rating = stats['avg_rating']

        book = db_session.get(book_model, book_id)
        if book:
            book.rating = avg_rating
            db_session.commit()
            logger.info(f"Updated rating for book {book_id}: {avg_rating}")
            return True
        else:
            logger.warning(f"Book {book_id} not found in PostgreSQL")
            return False

    except Exception as e:
        logger.error(f"Error updating book rating: {e}")
        db_session.rollback()
        return False


def compute_order_total(book_price, quantity, promotions):

    best_total = book_price * quantity

    for promo in promotions:
        discount_type = promo.get('discount_type')
        if discount_type == 'percent':
            discount_percent = promo.get('discount_value', 0)
            total = book_price * quantity * (1 - discount_percent / 100)
            best_total = min(best_total, total)

        elif discount_type == 'buy_x_get_y_free':
            buy = promo.get('buy_quantity', 0)
            free = promo.get('free_quantity', 0)
            if buy <= 0 or free <= 0:
                continue
            free_books = (quantity // (buy + free)) * free
            paid_quantity = quantity - free_books
            total = book_price * paid_quantity
            best_total = min(best_total, total)


    return best_total

def get_active_promotions_for_book(book_id, genre_id):
    now = datetime.utcnow()
    return list(promotions_collection.find({
        'valid_until': {'$gte': now},
        '$or': [
            {'target_type': 'book', 'target_id': book_id},
            {'target_type': 'genre', 'target_id': genre_id}
        ]
    }))

def compute_best_price(book_price, quantity, promotions):
    base_total = book_price * quantity
    best_total = base_total
    best_promo = None
    for promo in promotions:
        if promo.get('discount_type') == 'percent':
            disc = promo.get('discount_value', 0)
            total = base_total * (1 - disc / 100)
        elif promo.get('discount_type') == 'buy_x_get_y_free':
            buy = promo.get('buy_quantity', 0)
            free = promo.get('free_quantity', 0)
            if buy <= 0 or free <= 0:
                continue
            free_books = (quantity // (buy + free)) * free
            paid_quantity = quantity - free_books
            total = book_price * paid_quantity
        else:
            continue
        if total < best_total:
            best_total = total
            best_promo = promo
    return best_total, best_promo