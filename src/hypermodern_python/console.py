"""Command-line interface."""
import sys

import click
import code
from functools import partial

from mysql_mimic import MysqlServer
import polars as pl
import pandas as pd

from datetime import date, datetime

# TO allow pyinstaller to find imports
import pyarrow as pa
import xlsxwriter
import numpy as np
import kola

import duckdb
import _thread as thread

from polars import DataFrame

from src.hypermodern_python.queryprocessor import QueryProcessor
from src.hypermodern_python.qwebserv import start_web
from .mysession import Session
from . import __version__


@click.command()
@click.option(
    "--language",
    "-l",
    default="en",
    help="Language edition of Wikipedia",
    metavar="LANG",
    show_default=True,
)
@click.version_option(version=__version__)
def main(language: str) -> None:
    """The hypermodern Python project."""
    click.secho("QuantDB", fg="green")
    # https://bernsteinbear.com/blog/simple-python-repl/
    print("Launching......")
    queryProcessor = QueryProcessor()
    thread.start_new_thread(start_web, (queryProcessor, 8080))
    thread.start_new_thread(start_sql, (queryProcessor, 3306))
    repl = Repl(queryProcessor)
    repl.interact(banner="", exitmsg="")



if __name__ == "__main__":
    main()


class Repl(code.InteractiveConsole):
    def __init__(self, queryProcessor: QueryProcessor):
        super().__init__()
        self.queryProcessor = queryProcessor
        sys.ps1 = self.queryProcessor.getps1()

    def runsource(self, source: str, filename="<input>", symbol="single"):
        if len(source.strip()) == 0:
            return
        try:
            r = self.queryProcessor.queryraw(source)
            if r is not None:
                print(r)
        except Exception as error:
            print(error)
        sys.ps1 = self.queryProcessor.getps1()

class MySession(Session):
    def __init__(self, queryProcessor: QueryProcessor):
        super().__init__()
        self.queryProcessor = queryProcessor

    # 22
    # 3.3
    # complex(5, 3)
    # "ab"
    # [2, 3]
    # ("pp", 22)
    # range(4)
    # {"a": 11, "b": 12}
    # True
    # {"apple", "banana", "cherry"}

    async def query(self, expression, sql, attrs):
        r = self.queryProcessor.query(sql)
        if isinstance(r, pl.DataFrame):
            return r.rows(), r.columns

        return [("a", 1), ("b", 2)], ["col1", "col2"]

    async def schema(self):
        # Optionally provide the database schema.
        # This is used to serve INFORMATION_SCHEMA and SHOW queries.
        return {
            "table": {
                "col1": "TEXT",
                "col2": "INT",
            }
        }


def start_sql(queryProcessor: QueryProcessor, port: int):
    print("Starting MySQL Server port: ", port)
    handler = partial(MySession, queryProcessor)
    server = MysqlServer(session_factory=handler, port=port)
    asyncio.run(server.serve_forever())

