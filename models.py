from flask_sqlalchemy import SQLAlchemy
from datetime import datetime, timedelta

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import CheckConstraint, UniqueConstraint, func
from datetime import datetime

db = SQLAlchemy()


class Genre(db.Model):
    __tablename__ = 'genres'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False, unique=True)
    description = db.Column(db.Text)
    popularity = db.Column(db.Float, default=0.0)
    book_count = db.Column(db.Integer, default=0)

    def __repr__(self):
        return f'<Genre {self.name}>'


class Book(db.Model):
    __tablename__ = 'books'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(200), nullable=False)
    author = db.Column(db.String(200), nullable=False)
    price = db.Column(db.Numeric(10, 2), nullable=False)
    age_rating = db.Column(db.String(10), nullable=False)
    rating = db.Column(db.Float, default=0.0)
    stock = db.Column(db.Integer, nullable=False, default=0)
    genre_id = db.Column(db.Integer, db.ForeignKey('genres.id'), nullable=False)

    pages = db.Column(db.Integer)
    cover_type = db.Column(db.String(50))
    publisher = db.Column(db.String(200))
    description = db.Column(db.Text)


    genre = db.relationship('Genre', backref='books', lazy=True)

    __table_args__ = (
        db.CheckConstraint('price > 0', name='price_positive'),
        db.CheckConstraint('stock >= 0', name='stock_non_negative'),
        db.CheckConstraint('rating >= 0 AND rating <= 5', name='rating_range'),
        db.CheckConstraint('pages IS NULL OR pages > 0', name='pages_positive'),
        db.CheckConstraint(
            'cover_type IS NULL OR cover_type IN ("Твердая", "Мягкая", "Суперобложка", "Кожаный переплет")',
            name='cover_type_valid'
        ),
    )

    def __repr__(self):
        return f'<Book {self.title}>'


class OrderHistory(db.Model):
    __tablename__ = 'order_history'
    id = db.Column(db.Integer, primary_key=True)
    book_id = db.Column(db.Integer, db.ForeignKey('books.id'), nullable=False)
    quantity = db.Column(db.Integer, nullable=False)
    order_date = db.Column(db.DateTime, default=datetime.utcnow)
    total_price = db.Column(db.Numeric(10, 2), nullable=False, default=0.0)

    book = db.relationship('Book', backref='orders')

    __table_args__ = (
        CheckConstraint('quantity > 0', name='quantity_positive'),
    )


