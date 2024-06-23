"""Command-line interface."""
import textwrap

import click
import code
import asyncio
from functools import partial

from mysql_mimic import MysqlServer, Session, AllowedResult
import polars as pl
import pandas as pd
import os
import pyarrow
import xlsxwriter
import tempfile
import urllib.parse
import duckdb
import _thread as thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from polars.interchange.dataframe import PolarsDataFrame
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
    click.secho("HelloWOrld2", fg="green")
    #https://bernsteinbear.com/blog/simple-python-repl/
    print("Launching......")
    queryProcessor = QueryProcessor()
    thread.start_new_thread(start_web, (queryProcessor,))
    #start_sql(queryProcessor)
    repl = code.InteractiveConsole()
    repl.interact(banner="", exitmsg="")

if __name__ == "__main__":
     main()
    

class QueryProcessor:
    def query(self, sql) -> PolarsDataFrame:
        print(f"SQL string: {sql}")
        data = {"a": [1, 2], "b": [33, 41]}
        plx = pl.DataFrame(data)
        pdx = pd.DataFrame(data)
        d = {"plx": plx, "pdx": pdx }

        if sql.startswith("s)"):
            s = sql[2:]
            r = duckdb.sql(s).pl()
        else:
            r = eval(sql, {}, d)

        if isinstance(r, pd.DataFrame):
            r = pl.from_pandas(r)
        return r



class MySession(Session):
    def __init__(self, queryProcessor: QueryProcessor):
        self.queryProcessor = queryProcessor

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

    def query(self, qr:str) -> PolarsDataFrame:
        qry = urllib.parse.unquote(qr)
        return self.queryProcessor.query(qry)


    def do_GET(self):
        p = 'html' + self.path
        print(p)
        if self.path == '/':
           p = 'html/index.html'

        if(self.path.startswith("/?]")):
            r = self.query(self.path[3:])
            self.wfile.write(bytes(r._repr_html_(), 'utf-8'))
        elif(self.path.startswith("/file.csv?") or self.path.startswith("/t.csv?")
                or self.path.startswith("/file.xls?") or self.path.startswith("/t.xls?")):
            p = self.path.index("?")
            typ = self.path[p-3:p]
            r = self.query(self.path[p+1:])
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


def start_web(queryProcessor: QueryProcessor):
    print("Starting Webserver")
    handler = partial(Serv, queryProcessor)
    httpd = HTTPServer(('localhost',8080), handler)
    httpd.serve_forever()

def start_sql(queryProcessor: QueryProcessor):
    print("Starting MySQL Server")
    server = MysqlServer(session_factory=MySession(queryProcessor))
    asyncio.run(server.serve_forever())

