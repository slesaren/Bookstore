from flask import Flask, render_template, request, redirect, url_for, flash, abort, jsonify
from sqlalchemy import func, desc
from datetime import datetime, timedelta
import redis
import json
import logging
import re
import sys
from mongo_utils import get_book_rating_stats, get_popular_books_advanced, update_book_rating, compute_best_price, \
    get_active_promotions_for_book
from m import update_genre_popularity_on_order, get_genre_popularity_stats
import os

from config import Config
from models import db, Book, Genre, OrderHistory
from mongo_models import reviews_collection, promotions_collection
from redis_utils import RedisBookStats
from mongo_utils import get_book_rating_stats, get_popular_books_advanced
from mongo_utils import compute_order_total


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

if sys.version_info[0] < 3:
    print("Error: Python 3 is required")
    sys.exit(1)

redis_client = None
redis_stats = None
try:
    redis_client = redis.Redis(
        host=Config.REDIS_HOST,
        port=Config.REDIS_PORT,
        db=Config.REDIS_DB,
        password=Config.REDIS_PASSWORD,
        socket_timeout=Config.REDIS_SOCKET_TIMEOUT,
        decode_responses=True
    )
    redis_client.ping()
    redis_stats = RedisBookStats(redis_client)
    logger.info("Redis connected successfully")
except Exception as e:
    logger.warning(f"Redis connection failed: {e}")
    redis_stats = None

app = Flask(__name__)
app.config.from_object(Config)
db.init_app(app)

with app.app_context():
    try:
        db.create_all()
        logger.info("Database tables created/verified")
    except Exception as e:
        logger.error(f"Database creation error: {e}")

def get_redis_or_fetch(key, fetch_func, ttl=60):
    if redis_client is None:
        logger.info(f"Redis unavailable, fetching data directly for key: {key}")
        try:
            result = fetch_func()
            if result is None:
                logger.warning(f"Fetch function returned None for key: {key}")
                return []
            return result
        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            return []

    try:
        cached = redis_client.get(key)
        if cached:
            logger.info(f"Cache hit for key: {key}")
            return json.loads(cached)
    except Exception as e:
        logger.warning(f"Redis get error: {e}")

    try:
        data = fetch_func()
        if data:
            redis_client.setex(key, ttl, json.dumps(data))
            logger.info(f"Cached data for key: {key} with TTL {ttl}")
            return data
        else:
            logger.warning(f"No data to cache for key: {key}")
            return []
    except Exception as e:
        logger.error(f"Database error: {e}")
        return []


def compute_order_total_with_promo(book_price, quantity, promotions):

    base_total = book_price * quantity
    best_total = base_total
    best_promo = None

    for promo in promotions:
        discount_type = promo.get('discount_type')

        if discount_type == 'percent':
            discount_value = promo.get('discount_value', 0)
            total = base_total * (1 - discount_value / 100)
            if total < best_total:
                best_total = total
                best_promo = promo

        elif discount_type == 'buy_x_get_y_free':
            buy = promo.get('buy_quantity', 0)
            free = promo.get('free_quantity', 0)
            if buy <= 0 or free <= 0:
                continue
            free_books = (quantity // (buy + free)) * free
            paid_quantity = quantity - free_books
            total = book_price * paid_quantity
            if total < best_total:
                best_total = total
                best_promo = promo

    return best_total, best_promo




def enrich_book_with_stats(book):
    if not book:
        return book

    if redis_stats is not None:
        redis_views = redis_stats.get_views(book.id)
        if redis_views > 0:
            book.views_count = redis_views

    if redis_stats is not None:
        avg_rating = (redis_stats.

                      get_cached_rating(book.id))
        if avg_rating is None and reviews_collection is not None:
            stats = get_book_rating_stats(reviews_collection, book.id)
            avg_rating = stats['avg_rating']
            redis_stats.cache_rating(book.id, avg_rating)
        book.avg_rating = avg_rating if avg_rating else 0
    elif reviews_collection is not None:
        stats = get_book_rating_stats(reviews_collection, book.id)
        book.avg_rating = stats['avg_rating']
    else:
        book.avg_rating = 0
    return book

@app.route('/')
def index():
    def fetch_popular_books():
        try:
            popular = db.session.query(
                Book.id, Book.title, Book.author, Book.price, Book.rating,
                func.coalesce(func.sum(OrderHistory.quantity), 0).label('total_ordered')
            ).outerjoin(OrderHistory, Book.id == OrderHistory.book_id) \
                .group_by(Book.id) \
                .order_by(desc('total_ordered'), desc(Book.rating)) \
                .limit(5).all()

            result = []
            for b in popular:
                book_data = {
                    'id': b[0],
                    'title': b[1],
                    'author': b[2],
                    'price': float(b[3]),
                    'rating': float(b[4])
                }
                if redis_stats is not None:
                    book_data['views'] = redis_stats.get_views(b[0])
                result.append(book_data)

            logger.info(f"Popular books loaded: {len(result)} books")
            return result
        except Exception as e:
            logger.error(f"Error fetching popular books: {e}")
            logger.exception("Detailed error:")
            return []

    popular_books = get_redis_or_fetch('popular_books', fetch_popular_books, ttl=120)

    if not popular_books:
        popular_books = []
        flash('Не удалось загрузить популярные книги, попробуйте позже.', 'error')

    return render_template('index.html', popular_books=popular_books)


@app.route('/book/<int:book_id>')
def book_detail(book_id):
    try:
        if redis_stats is not None:
            redis_stats.increment_views(book_id)

        book = db.session.get(Book, book_id)
        if not book:
            flash('Книга не найдена.', 'error')
            return redirect(url_for('index'))

        if book.genre_id:
            book.genre = db.session.get(Genre, book.genre_id)

        if redis_stats is not None:
            book.views = redis_stats.get_views(book_id)
        else:
            book.views = getattr(book, 'views_count', 0)

        if redis_stats is not None:
            avg_rating = redis_stats.get_cached_rating(book_id)
            if avg_rating is None and reviews_collection is not None:
                stats = get_book_rating_stats(reviews_collection, book_id)
                avg_rating = stats['avg_rating']
                redis_stats.cache_rating(book_id, avg_rating)
            book.avg_rating = avg_rating if avg_rating else 0
        elif reviews_collection is not None:
            stats = get_book_rating_stats(reviews_collection, book_id)
            book.avg_rating = stats['avg_rating']
        else:
            book.avg_rating = 0

    except Exception as e:
        logger.error(f"Error fetching book {book_id}: {e}")
        flash('Книга не найдена.', 'error')
        return redirect(url_for('index'))

    reviews = []
    avg_rating = None
    review_count = 0
    if reviews_collection is not None:
        try:
            reviews_cursor = reviews_collection.find({'book_id': book_id}).sort('date', -1).limit(10)
            reviews = list(reviews_cursor)
            for r in reviews:
                r['_id'] = str(r['_id'])

            stats = get_book_rating_stats(reviews_collection, book_id)
            avg_rating = stats['avg_rating']
            review_count = stats['review_count']
        except Exception as e:
            logger.error(f"MongoDB error: {e}")

    promotions = []
    if promotions_collection is not None:
        try:
            now = datetime.utcnow()
            promo_cursor = promotions_collection.find({
                'valid_until': {'$gte': now},
                '$or': [
                    {'target_type': 'book', 'target_id': book_id},
                    {'target_type': 'genre', 'target_id': book.genre_id}
                ]
            })
            promotions = list(promo_cursor)
            for p in promotions:
                p['_id'] = str(p['_id'])
                logger.info(f"Found promotion: {p['name']} for book {book_id}")
        except Exception as e:
            logger.error(f"MongoDB promotions error: {e}")

    return render_template('book_detail.html',
                           book=book,
                           reviews=reviews,
                           avg_rating=avg_rating,
                           review_count=review_count,
                           promotions=promotions)



@app.route('/popular')
@app.route('/popular/<int:limit>')
def popular(limit=10):
    if limit > 50:
        limit = 50

    popular_books = []

    if reviews_collection is not None and redis_stats is not None:
        try:
            aggregated = get_popular_books_advanced(
                reviews_collection=reviews_collection,
                order_history_model=Book,
                db_session=db.session,
                redis_stats=redis_stats,
                limit=limit,
                min_reviews=3
            )

            for item in aggregated:
                book = Book.query.get(item['id'])
                if book:
                    book.views = item.get('views', 0)
                    book.review_count = item.get('review_count', 0)
                    book.avg_rating = item.get('rating', 0)
                    popular_books.append(book)
            logger.info(f"Advanced popular books loaded: {len(popular_books)} books")
        except Exception as e:
            logger.error(f"Error in advanced popular query: {e}")

    if not popular_books:
        try:
            popular = db.session.query(
                Book.id, Book.title, Book.author, Book.price, Book.rating, Book.stock,
                func.coalesce(func.sum(OrderHistory.quantity), 0).label('total_ordered')
            ).outerjoin(OrderHistory, Book.id == OrderHistory.book_id) \
                .group_by(Book.id) \
                .order_by(desc('total_ordered'), desc(Book.rating)) \
                .limit(limit).all()

            for row in popular:
                book = Book.query.get(row[0])
                if book:
                    if redis_stats is not None:
                        book.views = redis_stats.get_views(book.id)
                    else:
                        book.views = 0
                    book.review_count = 0
                    book.avg_rating = book.rating
                    popular_books.append(book)
            logger.info(f"Simple popular books loaded: {len(popular_books)} books")
        except Exception as e:
            logger.error(f"Popular query error: {e}")
            flash('Ошибка загрузки популярных книг.', 'error')
            popular_books = []

    return render_template('popular.html', books=popular_books, limit=limit)


@app.route('/add_review/<int:book_id>', methods=['GET', 'POST'])
def add_review(book_id):
    try:
        book = db.session.get(Book, book_id)
        if not book:
            flash('Книга не найдена.', 'error')
            return redirect(url_for('index'))
    except Exception as e:
        logger.error(f"Error fetching book {book_id}: {e}")
        flash('Книга не найдена.', 'error')
        return redirect(url_for('index'))

    if reviews_collection is None:
        flash('Система отзывов временно недоступна.', 'error')
        return redirect(url_for('book_detail', book_id=book_id))

    if request.method == 'POST':
        author = request.form.get('author', '').strip()
        rating = request.form.get('rating', type=int)
        review_text = request.form.get('review_text', '').strip()

        errors = []

        author_pattern = r'^[A-Za-zА-Яа-яЁё\s-]{2,50}$'
        if not author:
            errors.append('Имя автора обязательно для заполнения.')
        elif not re.match(author_pattern, author):
            errors.append('Имя автора должно содержать только буквы, пробелы и дефисы (2-50 символов).')

        if rating is None:
            errors.append('Пожалуйста, выберите рейтинг.')
        elif rating < 1 or rating > 5:
            errors.append('Рейтинг должен быть от 1 до 5.')

        text_pattern = r'^[\s\S]{10,1000}$'
        if not review_text:
            errors.append('Текст отзыва обязателен для заполнения.')
        elif not re.match(text_pattern, review_text):
            errors.append('Текст отзыва должен содержать от 10 до 1000 символов.')

        forbidden_pattern = r'[<>{}|\\]'
        if re.search(forbidden_pattern, review_text):
            errors.append('Текст отзыва содержит недопустимые символы.')

        if errors:
            for error in errors:
                flash(error, 'error')
        else:
            try:
                review = {
                    'book_id': book_id,
                    'rating': rating,
                    'author': author,
                    'text': review_text,
                    'date': datetime.utcnow()
                }

                reviews_collection.insert_one(review)
                update_book_rating(db.session, Book, reviews_collection, book_id)

                if redis_stats is not None:
                    redis_stats.delete_cached_rating(book_id)

                flash('Спасибо! Ваш отзыв успешно добавлен.', 'success')
                return redirect(url_for('book_detail', book_id=book_id))

            except Exception as e:
                logger.error(f"Error saving review: {e}")
                flash('Ошибка при сохранении отзыва. Пожалуйста, попробуйте позже.', 'error')

    return render_template('add_review.html', book=book)


@app.route('/search', methods=['GET', 'POST'])
def search():
    books = []
    if request.method == 'POST':
        title = request.form.get('title', '').strip()
        author = request.form.get('author', '').strip()
        genre_id = request.form.get('genre_id', type=int)

        query = Book.query
        if title:
            query = query.filter(Book.title.ilike(f'%{title}%'))
        if author:
            query = query.filter(Book.author.ilike(f'%{author}%'))
        if genre_id:
            query = query.filter(Book.genre_id == genre_id)

        try:
            books = query.all()
            for book in books:
                if redis_stats is not None:
                    book.views = redis_stats.get_views(book.id)
                else:
                    book.views = getattr(book, 'views_count', 0)

                if redis_stats is not None:
                    avg_rating = redis_stats.get_cached_rating(book.id)
                    if avg_rating is None and reviews_collection is not None:
                        stats = get_book_rating_stats(reviews_collection, book.id)
                        avg_rating = stats['avg_rating']
                        redis_stats.cache_rating(book.id, avg_rating)
                    book.avg_rating = avg_rating if avg_rating else 0
                elif reviews_collection is not None:
                    stats = get_book_rating_stats(reviews_collection, book.id)
                    book.avg_rating = stats['avg_rating']
                else:
                    book.avg_rating = 0
            logger.info(f"Search found {len(books)} books")
        except Exception as e:
            logger.error(f"Search error: {e}")
            flash('Ошибка поиска, повторите позже.', 'error')

    try:
        genres = Genre.query.all()
    except Exception as e:
        logger.error(f"Error fetching genres: {e}")
        genres = []
        flash('Ошибка загрузки жанров.', 'error')

    return render_template('search.html', books=books, genres=genres)


@app.route('/order/<int:book_id>', methods=['GET', 'POST'])
def order_book(book_id):
    try:
        book = db.session.get(Book, book_id)
        if not book:
            flash('Книга не найдена.', 'error')
            return redirect(url_for('index'))

        if redis_stats is not None:
            book.views = redis_stats.get_views(book_id)

        now = datetime.utcnow()
        promotions = list(promotions_collection.find({
            'valid_until': {'$gte': now},
            '$or': [
                {'target_type': 'book', 'target_id': book_id},
                {'target_type': 'genre', 'target_id': book.genre_id}
            ]
        }))

    except Exception as e:
        logger.error(f"Error fetching book {book_id}: {e}")
        flash('Книга не найдена.', 'error')
        return redirect(url_for('index'))

    if request.method == 'POST':
        quantity = request.form.get('quantity', type=int)
        if not quantity or quantity <= 0:
            flash('Введите корректное количество.', 'error')
        elif quantity > book.stock:
            flash(f'На складе только {book.stock} экземпляров.', 'error')
        else:
            try:
                total_price, applied_promo = compute_order_total_with_promo(float(book.price), quantity, promotions)

                order = OrderHistory(book_id=book.id, quantity=quantity, total_price=total_price)
                db.session.add(order)
                book.stock -= quantity

                db.session.commit()
                logger.info(f"Order created: book {book_id}, quantity {quantity}")

                success = update_genre_popularity_on_order(
                    db.session, Genre, Book, OrderHistory, book_id, time_period_days=60
                )

                if success:
                    logger.info(f"Genre popularity updated after order for book {book_id}")
                else:
                    logger.warning(f"Failed to update genre popularity for book {book_id}")

                flash(f'Заказ на {quantity} экз. "{book.title}" оформлен! Итого: {total_price:.2f} ₽', 'success')
                return redirect(url_for('index'))

            except Exception as e:
                db.session.rollback()
                logger.error(f"Order error: {e}")
                logger.exception("Detailed error:")
                flash('Ошибка при оформлении заказа.', 'error')
                return redirect(url_for('book_detail', book_id=book_id))

    default_total, applied_promo = compute_order_total_with_promo(float(book.price), 1, promotions)

    return render_template('order.html',
                           book=book,
                           promotions=promotions,
                           default_total=default_total,
                           applied_promotion=applied_promo['name'] if applied_promo else None)




@app.route('/genre/<int:genre_id>')
def genre_books(genre_id):
    try:
        genre = db.session.get(Genre, genre_id)
        if not genre:
            flash('Жанр не найден.', 'error')
            return redirect(url_for('genres'))

        books = Book.query.filter_by(genre_id=genre_id).all()
        for book in books:
            if redis_stats is not None:
                book.views = redis_stats.get_views(book.id)
            else:
                book.views = getattr(book, 'views_count', 0)

            if redis_stats is not None:
                avg_rating = redis_stats.get_cached_rating(book.id)
                if avg_rating is None and reviews_collection is not None:
                    stats = get_book_rating_stats(reviews_collection, book.id)
                    avg_rating = stats['avg_rating']
                    redis_stats.cache_rating(book.id, avg_rating)
                book.avg_rating = avg_rating if avg_rating else 0
            elif reviews_collection is not None:
                stats = get_book_rating_stats(reviews_collection, book.id)
                book.avg_rating = stats['avg_rating']
            else:
                book.avg_rating = 0
    except Exception as e:
        logger.error(f"Error fetching genre {genre_id}: {e}")
        flash('Жанр не найден.', 'error')
        return redirect(url_for('genres'))

    return render_template('genre_books.html', genre=genre, books=books)

@app.route('/genres')
def genres():
    try:
        genres_list = Genre.query.all()
        logger.info(f"Genres loaded: {len(genres_list)} genres")
    except Exception as e:
        logger.error(f"Error fetching genres: {e}")
        flash('Ошибка загрузки жанров.', 'error')
        genres_list = []

    return render_template('genres.html', genres=genres_list)



@app.errorhandler(404)
def not_found(e):
    return render_template('error.html', message='Страница не найдена'), 404


@app.errorhandler(500)
def internal_error(e):
    logger.error(f"500 error: {e}")
    return render_template('error.html', message='Внутренняя ошибка сервера. Пожалуйста, попробуйте позже.'), 500


@app.route('/debug')
def debug():
    books = Book.query.all()
    genres = Genre.query.all()
    orders = OrderHistory.query.all()
    return f"Books: {len(books)}<br>Genres: {len(genres)}<br>Orders: {len(orders)}"


@app.route('/debug/books')
def debug_books():
    try:
        books = Book.query.all()
        books = [enrich_book_with_stats(book) for book in books]
        html = '<h2>Список книг:</h2><ul>'
        for book in books:
            html += f'<li>ID: {book.id}, Title: {book.title}, Stock: {book.stock}, Views: {getattr(book, "views", 0)}'
            html += f' - <a href="/book/{book.id}">Детали</a>'
            html += f' - <a href="/order/{book.id}">Заказать</a></li>'
        html += '</ul>'
        html += f'<p>Всего книг: {len(books)}</p>'
        return html
    except Exception as e:
        return f'Error: {e}'


@app.route('/debug/redis')
def debug_redis():
    if redis_stats is None:
        return "Redis is not available"

    result = "<h2>Redis Debug Info</h2>"

    result += "<h3>Book views:</h3><ul>"
    for book_id in [1, 2, 3, 5]:
        views = redis_stats.get_views(book_id)
        result += f"<li>Book {book_id}: {views} views</li>"
    result += "</ul>"

    result += "<h3>Popular books from Redis:</h3><ul>"
    popular = redis_stats.get_popular_books(10)
    for item in popular:
        result += f"<li>Book {item['id']}: {item['views']} views</li>"
    result += "</ul>"

    return result

@app.route('/calculate_total/<int:book_id>')
def calculate_total(book_id):
    quantity = request.args.get('quantity', type=int)
    if not quantity or quantity <= 0:
        return jsonify({'error': 'Неверное количество'}), 400
    book = db.session.get(Book, book_id)
    if not book:
        return jsonify({'error': 'Книга не найдена'}), 404
    promotions = get_active_promotions_for_book(book_id, book.genre_id)
    total, promo = compute_best_price(float(book.price), quantity, promotions)
    return jsonify({'total': total, 'applied_promotion': promo['name'] if promo else None})

if __name__ == '__main__':
    logger.info("Starting Flask application...")
    app.run(debug=True, host='127.0.0.1', port=5000)