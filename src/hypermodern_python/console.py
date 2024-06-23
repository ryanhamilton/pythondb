"""Command-line interface."""
import textwrap

import click
import code
import asyncio
from functools import partial

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
    click.secho("HelloWOrld", fg="green")
    #https://bernsteinbear.com/blog/simple-python-repl/
    repl = code.InteractiveConsole()
    repl.interact(banner="", exitmsg="")

    
    