import json

import sqlalchemy
from sqlalchemy.orm import sessionmaker

from models import create_tables, drop_tables, Publisher, Book, Shop, Stock, Sale

type_db = 'postgresql'
login = 'postgres'
password = '89345672'
name_db = 'book_sell_db'

DSN = f'{type_db}://{login}:{password}@localhost:5432/{name_db}'
engine = sqlalchemy.create_engine(DSN)

Session = sessionmaker(bind=engine)
session = Session()

def load_data():
    with open('fixtures/tests_data.json', 'r') as fd:
        data = json.load(fd)

    with Session() as session:
        for record in data:
            model = {
                'publisher': Publisher,
                'shop': Shop,
                'book': Book,
                'stock': Stock,
                'sale': Sale,
            }.get(record.get('model'))

            if model:
                existing_record = session.query(model).filter_by(id=record.get('pk')).first()
                if not existing_record:
                    session.add(model(id=record.get('pk'), **record.get('fields')))
                else:
                    print(f"Запись для {record.get('model')} с ID {record.get('pk')} уже существует.")
            else:
                print(f"Модель '{record.get('model')}' не найдена")

        try:
            session.commit()
        except Exception as e:
            print(f"Ошибка при сохранении изменений: {e}")
            session.rollback()

def get_purchases_by_publisher(publisher_name):
    with Session() as session:
        publisher = session.query(Publisher).filter_by(name=publisher_name).first()

        if publisher:
            results = (session.query(
                Book.title,
                Shop.name,
                Sale.price,
                Sale.date_sale
            )
            .select_from(Sale)  
            .join(Stock)
            .join(Book)
            .join(Shop)  
            .filter(Book.id_publisher == publisher.id)
            .all())

            print(f"Факты покупки книг издателя '{publisher.name}':")
            for title, shop_name, price, date in results:
                print(f"{title} | {shop_name} | {price} | {date.strftime('%d-%m-%Y')}")
        else:
            print("Издатель не найден")


if __name__ == "__main__":
    create_tables(engine)
    load_data()
    publisher_name = input("Введите имя издателя: ")
    get_purchases_by_publisher(publisher_name)
    drop_tables(engine)
