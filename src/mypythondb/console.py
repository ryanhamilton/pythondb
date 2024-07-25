"""Command-line interface."""
import sys

import click
from click.core import ParameterSource
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
@click.argument('filepaths', nargs=-1)
@click.version_option(version=__version__)
def main(filepaths: tuple[str], language: str, port: int, webport: int, command: str, quiet: bool, verbose: bool) -> None:

    lang = language
    parameter_source = click.get_current_context().get_parameter_source('language')
    if parameter_source == ParameterSource.DEFAULT:
        lang = None

    """ PythonDB interactive SQL/python querying."""
    if not quiet:
        click.secho(BANNER, fg="blue")
        click.echo("PythonDB " + __version__ + " https://github.com/ryanhamilton/pythondb")
        click.echo("web: http://localhost:" + str(webport) + "    MySQL port: " + str(port))
    # https://bernsteinbear.com/blog/simple-python-repl/
    query_processor = start(filepaths, lang, port, webport, command, quiet, verbose)
    repl = Repl(query_processor, verbose)
    repl.interact(banner="", exitmsg="")


def start(filepaths: tuple[str] = (), language: str = "", port: int = 3306, webport: int = 8080, command: str = "", quiet: bool = False, verbose: bool = False) -> QueryProcessor:
    db = None
    lang = language
    source_files = []
    print(filepaths)
    for p in filepaths:
        if p.endswith(".duckdb") or p.endswith(".db"):
            db = duckdb.connect(p)
            if len(filepaths) == 1 and lang is None:
                lang = "dk"
        else:
            source_files.append(p)
    query_processor = QueryProcessor(verbose, db=db)
    if lang != "":
        query_processor.setlang(lang)
    thread.start_new_thread(start_web, (query_processor, webport))
    thread.start_new_thread(start_sql, (query_processor, port))

    query_processor.load_files(source_files)

    if command is not None:
        query_processor.queryraw(command)
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
