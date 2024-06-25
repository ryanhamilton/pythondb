"""Command-line interface."""
import ast
import textwrap
import sys
import click
import code
import asyncio
from functools import partial

from mysql_mimic import MysqlServer
import polars as pl
import pandas as pd
import os
from datetime import date,datetime

# TO allow pyinstaller to find imports
import pyarrow as pa
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


class QueryProcessor:
    def __init__(self):
        self.query_lang = "py"
        self.duckdb = duckdb.connect(":default:")
        data = {"a": [1, 2], "b": [33, 41]}
        plx = pl.DataFrame(data)
        pdx = pd.DataFrame(data)
        self.mylocals = {"plx": plx, "pdx": pdx, "qdb":self}
        self.ctx = pl.SQLContext(register_globals=True, eager=True, frames={"plx": plx})

    def setlang(self, lg:str):
        self.query_lang = lg

    def getconfig(self):
        return {"lang":self.query_lang}

    def getps1(self):
        return '>>>' if self.query_lang == 'py' else (self.query_lang + ">")

    def queryraw(self, sql):
        print(f"SQL string: {sql}")

        s = sql.strip()
        if len(s) == 0:
            return None
        elif s.startswith("qdb."): # Always run qdb as python. Handy to make commands standard?
            s = ">>>" + s
        elif len(s) < 3 or s[2] != '>': # Add default lang as prefix if none specified
            s = self.query_lang + '>' + s

        r = None
        if s.startswith("dk>"):
            q = s[3:]
            r = self.duckdb.sql(q)
            r = r.pl() if r is not None else None
        elif s.startswith("pl>"):
            q = s[3:]
            r = self.ctx.execute(q)
        elif s.startswith(">>>") or s.startswith("py>"):
            q = s[3:]
            r = exec_with_return(q, globals(), self.mylocals)
            print(r)
            # Register any newly created vars
            polars = {}
            for k in self.mylocals.keys():
                v = self.mylocals[k]
                if isinstance(v, pl.DataFrame):
                    self.duckdb.register(k, v)
                    polars[k] = v
                elif isinstance(v, pd.DataFrame):
                    self.duckdb.register(k, v)
            self.ctx = pl.SQLContext(register_globals=True, eager=True, frames=polars)
        else:
            print("Error unrecognised command.")

        return r

    def query(self, sql) -> DataFrame:
        return self.to_pdf(self.queryraw(sql))

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
            return pl.DataFrame({"list": obj})
        elif isinstance(obj, tuple):
            return pl.DataFrame({"tuple": [obj]})
        elif isinstance(obj, range):
            return pl.DataFrame({"range": [obj]})
        elif isinstance(obj, dict):
            return pl.from_dict(obj)
        elif isinstance(obj, set):
            return pl.DataFrame({"set": list(obj)})
        elif isinstance(obj, duckdb.DuckDBPyRelation):
            return obj.pl()
        if isinstance(obj, pd.DataFrame):
            return pl.from_pandas(obj)
        elif obj is None:
            return pl.DataFrame({"None": []})
        print(obj)
        return pl.DataFrame({"unrecognised": type(obj)})

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
    print("locals before: ", locals.keys())
    exec(ast.unparse(a), globals, locals)
    if last_expression:
        r = eval(last_expression, globals, locals)
        print("locals after: ", locals.keys())
        return r
