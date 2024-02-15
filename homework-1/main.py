"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv
from pathlib import Path

source = Path(Path(__file__).parent.parent, "north_data")


def get_tuples(filename) -> list[tuple]:
    lst = []
    with open(Path(source, filename), 'r') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)
        for read in csvreader:
            lst.append(tuple(read))
    return lst


customers = get_tuples("customers_data.csv")
employees = get_tuples("employees_data.csv")
orders = get_tuples("orders_data.csv")

conn = psycopg2.connect(
    host="localhost",
    database="north",
    user="postgres",
    password="12345"
)

try:
    with conn:
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO customers VALUES (%s, %s, %s)", customers)
            cur.executemany("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)", employees)
            cur.executemany("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)", orders)

finally:
    conn.close()
