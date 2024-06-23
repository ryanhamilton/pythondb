"""Command-line interface."""
import textwrap

import click
import code
import duckdb
import polars

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
