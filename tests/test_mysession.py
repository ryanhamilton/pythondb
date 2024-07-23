import mysql.connector
import pytest

import _thread as thread
import time
import json

from mysql.connector import FieldType
from mysql.connector.fabric import MySQLFabricConnection

from src.hypermodern_python.console import QueryProcessor
from src.hypermodern_python.mysession import start_sql
from tests.test_queryprocessor import tq


@pytest.fixture
def runner() -> None:
    query_processor = QueryProcessor()
    query_processor.setlang("py")
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

    s = "["
    for i in range(len(cursor.description)):
        desc = cursor.description[i]
        s = s + (desc[0] + ': ' + FieldType.get_info(desc[1]) + ', ')
    s = s + "]  "
    s = s + cursor.fetchall().__str__()
    return s


def chk(a: str, exp: str) -> None:
    assert fetchall(a) == exp


def test_two(runner: MySQLFabricConnection) -> None:
    assert fetchall('q)2+2') == "[int: LONGLONG, ]  [(4,)]"


def test_int() -> None:
    chk(tq[1], "[int: LONGLONG, ]  [(2,)]")


def test_float() -> None:
    chk(tq[2], "[float: DOUBLE, ]  [(3.45,)]")


def test_complex() -> None:
    chk(tq[3], "[complex: VARCHAR, ]  [('(6+7j)',)]")


def test_str() -> None:
    chk(tq[4], "[str: STRING, ]  [('stringy',)]")


def test_list() -> None:
    chk(tq[5], '[list: LONGLONG, ]  [(3,), (5,)]')


def test_dict() -> None:
    chk(tq[6], "[a: LONGLONG, b: DOUBLE, c: STRING, ]  [(11, 12.5, 'st')]")


def test_bool() -> None:
    chk(tq[7], '[bool: TINY, ]  [(1,)]')


def test_set() -> None:
    chk(tq[8], "[set: STRING, ]  [('cherry',)]")


def test_pandas() -> None:
    chk(tq[9], "[Name: STRING, Age: LONGLONG, ]  [('tom', 10), ('nick', 11), ('juli', 14)]")


def test_python_duckdb() -> None:
    chk(tq[10], '[i: LONGLONG, ]  [(42,)]')

