import logging
from sqlalchemy import func
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def update_genre_popularity_on_order(db_session, Genre, Book, OrderHistory, book_id, time_period_days=60):

    try:

        book = db_session.get(Book, book_id)

        if not book:
            logger.warning(f"Book {book_id} not found")
            return False

        genre_id = book.genre_id
        start_date = datetime.utcnow() - timedelta(days=time_period_days)

        all_genre_orders = db_session.query(
            Genre.id,
            func.coalesce(func.sum(OrderHistory.quantity), 0).label('total_orders')
        ).outerjoin(
            Book, Book.genre_id == Genre.id
        ).outerjoin(
            OrderHistory, OrderHistory.book_id == Book.id
        ).filter(
            (OrderHistory.order_date >= start_date) | (OrderHistory.order_date.is_(None))
        ).group_by(
            Genre.id
        ).all()

        max_orders = max([g.total_orders for g in all_genre_orders]) if all_genre_orders else 1


        for genre_data in all_genre_orders:
            if max_orders > 0:
                popularity = (genre_data.total_orders / max_orders) * 10
            else:
                popularity = 0

            db_session.query(Genre).filter(Genre.id == genre_data.id).update({
                'popularity': round(popularity, 1)
            })
            logger.debug(
                f"Updated genre {genre_data.id}: orders={genre_data.total_orders}, popularity={popularity:.1f}")

        db_session.commit()
        logger.info(f"Genre popularity updated after order for book {book_id}")
        return True

    except Exception as e:
        logger.error(f"Error updating genre popularity on order: {e}")
        db_session.rollback()
        return False


def recalc_all_genre_popularity(db_session, Genre, Book, OrderHistory, time_period_days=60):
    try:
        start_date = datetime.utcnow() - timedelta(days=time_period_days)

        genre_orders = db_session.query(
            Genre.id,
            Genre.name,
            func.coalesce(func.sum(OrderHistory.quantity), 0).label('total_orders')
        ).outerjoin(
            Book, Book.genre_id == Genre.id
        ).outerjoin(
            OrderHistory, OrderHistory.book_id == Book.id
        ).filter(
            (OrderHistory.order_date >= start_date) | (OrderHistory.order_date.is_(None))
        ).group_by(
            Genre.id, Genre.name
        ).all()

        if not genre_orders:
            logger.warning("No genre orders found")
            return False

        max_orders = max([g.total_orders for g in genre_orders])

        for genre in genre_orders:
            if max_orders > 0:
                popularity = (genre.total_orders / max_orders) * 10
            else:
                popularity = 0

            db_session.query(Genre).filter(Genre.id == genre.id).update({
                'popularity': round(popularity, 1)
            })
            logger.info(f"Updated genre {genre.name}: orders={genre.total_orders}, popularity={popularity:.1f}")

        db_session.commit()
        logger.info(f"All genres popularity recalculated: {len(genre_orders)} genres")
        return True

    except Exception as e:
        logger.error(f"Error recalculating genre popularity: {e}")
        db_session.rollback()
        return False


def get_genre_popularity_stats(db_session, Genre, Book, OrderHistory, time_period_days=60):
    try:
        start_date = datetime.utcnow() - timedelta(days=time_period_days)

        stats = db_session.query(
            Genre.id,
            Genre.name,
            Genre.popularity,
            func.count(Book.id).label('book_count'),
            func.coalesce(func.sum(OrderHistory.quantity), 0).label('total_orders')
        ).outerjoin(
            Book, Book.genre_id == Genre.id
        ).outerjoin(
            OrderHistory, (OrderHistory.book_id == Book.id) & (OrderHistory.order_date >= start_date)
        ).group_by(
            Genre.id, Genre.name, Genre.popularity
        ).order_by(
            func.coalesce(func.sum(OrderHistory.quantity), 0).desc()
        ).all()

        return stats

    except Exception as e:
        logger.error(f"Error getting genre popularity stats: {e}")
        return []