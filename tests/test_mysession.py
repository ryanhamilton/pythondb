import mysql
import mysql.connector
import pytest

import _thread as thread
import time
import json
from mysql.connector.fabric import MySQLFabricConnection

from src.hypermodern_python.console import QueryProcessor
from src.hypermodern_python.mysession import start_sql


@pytest.fixture
def runner() -> None:
    query_processor = QueryProcessor()
    thread.start_new_thread(start_sql, (query_processor, 3306))
    time.sleep(5)


def fetchone(qry:str) -> None:
    connection = mysql.connector.connect(host="localhost", user="", password="",database="")
    cursor = connection.cursor()
    cursor.execute(qry)
    return cursor.fetchone().__str__()


def fetchall(qry:str) -> None:
    connection = mysql.connector.connect(host="localhost", user="", password="",database="")
    cursor = connection.cursor()
    cursor.execute(qry)
    return cursor.fetchall().__str__()


def test_two(runner: MySQLFabricConnection) -> None:
    assert fetchall('q)2+2') == '[(4,)]'
