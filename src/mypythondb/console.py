"""Command-line interface."""
import sys

import click
import code

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

from src.mypythondb.queryprocessor import QueryProcessor
from src.mypythondb.qwebserv import start_web
from .mysession import Session, start_sql
from . import __version__


BANNER = """
██████╗ ██╗   ██╗████████╗██╗  ██╗ ██████╗ ███╗   ██╗██████╗ ██████╗ 
██╔══██╗╚██╗ ██╔╝╚══██╔══╝██║  ██║██╔═══██╗████╗  ██║██╔══██╗██╔══██╗
██████╔╝ ╚████╔╝    ██║   ███████║██║   ██║██╔██╗ ██║██║  ██║██████╔╝
██╔═══╝   ╚██╔╝     ██║   ██╔══██║██║   ██║██║╚██╗██║██║  ██║██╔══██╗
██║        ██║      ██║   ██║  ██║╚██████╔╝██║ ╚████║██████╔╝██████╔╝
╚═╝        ╚═╝      ╚═╝   ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═════╝ ╚═════╝ 
"""

@click.command()
@click.option("--language", "-l", default="PYTHON",type=click.Choice(['DK','PY','PL','DUCKDB','PYTHON','POLARS'], case_sensitive=False),
              help="Language to interpret code as.",metavar="LANG", show_default=True)
@click.option("--command", "-c", help="Run COMMAND", metavar="COMMAND")
@click.option("--port", "-P", help="Port for MySQL compatible server to listen on", metavar="SQLPORT", default=3306)
@click.option("--webport", "-w", help="Port for webserver to listen on", metavar="WEBPORT", default=8080)
@click.option("--quiet", "-q", help="Quiet, don't show banner", default=False, is_flag=True)
@click.option("--verbose", "-v", help="Display debugging information", default=False, is_flag=True)
#@click.option("--duckdbfile", "-d", help="DuckDB File to open as database", metavar="FILE")
@click.version_option(version=__version__)
def main(language: str, port: int, webport: int, command: str, quiet: bool, verbose: bool) -> None:
    """ PythonDB interactive SQL/python querying."""
    if not quiet:
        click.secho(BANNER, fg="blue")
        click.echo("PythonDB " + __version__ + " https://github.com/ryanhamilton/pythondb")
        click.echo("web: http://localhost:" + str(webport) + "    MySQL port: " + str(port))
    # https://bernsteinbear.com/blog/simple-python-repl/
    query_processor = start(language, port, webport, command, quiet, verbose)
    repl = Repl(query_processor, verbose)
    if command is not None:
        query_processor.queryraw(command)
    repl.interact(banner="", exitmsg="")


def start(language: str, port: int, webport: int, command: str, quiet: bool, verbose: bool) -> QueryProcessor:
    query_processor = QueryProcessor(verbose)
    query_processor.setlang(language)
    thread.start_new_thread(start_web, (query_processor, webport))
    thread.start_new_thread(start_sql, (query_processor, port))
    return query_processor


if __name__ == "__main__":
    main()


class Repl(code.InteractiveConsole):
    def __init__(self, queryProcessor: QueryProcessor, verbose: bool):
        super().__init__()
        self.queryProcessor = queryProcessor
        self.verbose = verbose
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