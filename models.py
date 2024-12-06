import sqlalchemy as sq
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

class Publisher(Base):
    __tablename__ = 'publisher'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)
    
    books = relationship("Book", back_populates="publisher")  

class Book(Base):
    __tablename__ = 'book'

    id = sq.Column(sq.Integer, primary_key=True)
    title = sq.Column(sq.String(length=100), nullable=False)  
    id_publisher = sq.Column(sq.Integer, sq.ForeignKey("publisher.id"), nullable=False)  
    
    publisher = relationship("Publisher", back_populates="books")  
    stocks = relationship("Stock", back_populates="book")  

class Shop(Base):
    __tablename__ = 'shop'

    id = sq.Column(sq.Integer, primary_key=True)
    name = sq.Column(sq.String(length=40), unique=True)

    stocks = relationship("Stock", back_populates="shop")  

class Stock(Base):
    __tablename__ = 'stock'

    id = sq.Column(sq.Integer, primary_key=True)
    id_book = sq.Column(sq.Integer, sq.ForeignKey("book.id"), nullable=False) 
    id_shop = sq.Column(sq.Integer, sq.ForeignKey("shop.id"), nullable=False)  
    count = sq.Column(sq.Integer, nullable=False)

    book = relationship("Book", back_populates="stocks")  
    shop = relationship("Shop", back_populates="stocks")  
    sales = relationship("Sale", back_populates="stock")  

class Sale(Base):
    __tablename__ = 'sale'

    id = sq.Column(sq.Integer, primary_key=True)
    price = sq.Column(sq.Float, nullable=False)  
    date_sale = sq.Column(sq.DateTime, nullable=False) 
    id_stock = sq.Column(sq.Integer, sq.ForeignKey("stock.id"), nullable=False)  
    count = sq.Column(sq.Integer, nullable=False)

    stock = relationship("Stock", back_populates="sales")  

def create_tables(engine):
    try:
        Base.metadata.create_all(engine)
        print("Таблицы успешно созданы.")
    except Exception as e:
        print(f"Ошибка при создании таблиц: {e}")   

def drop_tables(engine):
    Base.metadata.drop_all(engine)
    print("Все таблицы успешно удалены.")        