"""Command-line interface."""
import ast
import textwrap

import click
import code
import asyncio
from functools import partial

from mysql_mimic import MysqlServer
import polars as pl
import pandas as pd
import os

# TO allow pyinstaller to find imports
import pyarrow
import xlsxwriter
import numpy as np

import tempfile
import urllib.parse
import duckdb
import _thread as thread
from http.server import HTTPServer, BaseHTTPRequestHandler

from polars import DataFrame
from polars.interchange.dataframe import PolarsDataFrame

from .mysession import Session
from . import __version__, wikipedia


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
    #https://bernsteinbear.com/blog/simple-python-repl/
    print("Launching......")
    queryProcessor = QueryProcessor()
    thread.start_new_thread(start_web, (queryProcessor, 8080))
    thread.start_new_thread(start_sql, (queryProcessor, 3306))
    repl = code.InteractiveConsole()
    repl.interact(banner="", exitmsg="")


if __name__ == "__main__":
    main()


class QueryProcessor:
    def query(self, sql) -> DataFrame:
        print(f"SQL string: {sql}")
        data = {"a": [1, 2], "b": [33, 41]}
        plx = pl.DataFrame(data)
        pdx = pd.DataFrame(data)
        d = {"plx": plx, "pdx": pdx}

        if sql.startswith("s)"):
            s = sql[2:]
            r = duckdb.sql(s).pl()
        else:
            r = exec_with_return(sql, d, globals())

        if isinstance(r, pd.DataFrame):
            r = pl.from_pandas(r)
        return self.to_pdf(r)

    @staticmethod
    def to_pdf(obj) -> DataFrame:
        if isinstance(obj, pl.DataFrame):
            return obj
        elif isinstance(obj, bool):
            return pl.DataFrame({"bool": [obj]})
        elif isinstance(obj, int):
            return pl.DataFrame({"int": [obj]})
        elif isinstance(obj, float):
            return pl.DataFrame({"float": [obj]})
        elif isinstance(obj, complex):
            return pl.DataFrame({"complex": [obj]})
        elif isinstance(obj, str):
            return pl.DataFrame({"str": [obj]})
        elif isinstance(obj, list):
            return pl.DataFrame({"list": [obj]})
        elif isinstance(obj, tuple):
            return pl.DataFrame({"tuple": [obj]})
        elif isinstance(obj, range):
            return pl.DataFrame({"range": [obj]})
        elif isinstance(obj, dict):
            return pl.DataFrame(dict)
        elif isinstance(obj, set):
            return pl.DataFrame({"set": obj})
        return pl.DataFrame({"unrecognised": "unrecognised"})

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


class Serv(BaseHTTPRequestHandler):
    def __init__(self, queryProcessor: QueryProcessor, *args, **kwargs):
        self.queryProcessor = queryProcessor
        # BaseHTTPRequestHandler calls do_GET **inside** __init__ !!!
        # So we have to call super().__init__ after setting attributes.
        super().__init__(*args, **kwargs)

    extensions = {
        "html": "text/html",
        "css": "text/css",
        "js": "text/javascript",
        "plain": "text/plain",
        "xls": "application/vnd.ms-excel",
        "csv": "text/comma-separated-values"
    }

    def set_content_type(self):
        extension = self.path.split(".")[-1]
        if extension in self.extensions:
            return self.extensions[extension]
        return self.extensions["plain"]

    def query(self, qr: str) -> PolarsDataFrame:
        qry = urllib.parse.unquote(qr)
        return self.queryProcessor.query(qry)

    def do_GET(self):
        p = 'html' + self.path
        print(p)
        if self.path == '/':
            p = 'html/index.html'

        if (self.path.startswith("/?]")):
            r = self.query(self.path[3:])
            self.wfile.write(bytes(r._repr_html_(), 'utf-8'))
        elif (self.path.startswith("/file.csv?") or self.path.startswith("/t.csv?")
              or self.path.startswith("/file.xls?") or self.path.startswith("/t.xls?")):
            p = self.path.index("?")
            typ = self.path[p - 3:p]
            r = self.query(self.path[p + 1:])
            self.send_response(200)
            self.send_header("Content-type", self.extensions[typ])
            self.send_header("Content-Disposition", "attachment")
            self.end_headers()
            if typ == "xls":
                with tempfile.NamedTemporaryFile(suffix="xlsx") as tmp:
                    print(tmp.name)
                    r.write_excel(tmp.name)
                    self.wfile.write(bytes(open(tmp.name).read(), 'utf-8'))
            else:
                r.write_csv(self.wfile)
        elif os.path.exists(p):
            try:
                file_to_open = open(p).read()
                self.send_response(200)
                self.set_content_type()
            except:
                file_to_open = "File not found"
                self.send_response(404)
            self.end_headers()
            self.wfile.write(bytes(file_to_open, 'utf-8'))
        else:
            self.end_headers()
            self.wfile.write(b'Hello, World!')


def start_web(queryProcessor: QueryProcessor, port: int):
    print("Starting Webserver port: ", port)
    handler = partial(Serv, queryProcessor)
    httpd = HTTPServer(('localhost', port), handler)
    httpd.serve_forever()


def start_sql(queryProcessor: QueryProcessor, port: int):
    print("Starting MySQL Server port: ", port)
    handler = partial(MySession, queryProcessor)
    server = MysqlServer(session_factory=handler, port=port)
    asyncio.run(server.serve_forever())


def exec_with_return(code: str, globals: dict, locals: dict):
    a = ast.parse(code)
    last_expression = None
    if a.body:
        if isinstance(a_last := a.body[-1], ast.Expr):
            last_expression = ast.unparse(a.body.pop())
        elif isinstance(a_last, ast.Assign):
            last_expression = ast.unparse(a_last.targets[0])
        elif isinstance(a_last, (ast.AnnAssign, ast.AugAssign)):
            last_expression = ast.unparse(a_last.target)
    exec(ast.unparse(a), globals, locals)
    if last_expression:
        return eval(last_expression, globals, locals)
