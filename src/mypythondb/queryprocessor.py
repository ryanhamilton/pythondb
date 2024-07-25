"""Command-line interface."""
import ast
import sys
from http.server import HTTPServer

import click
import code
import asyncio
from functools import partial

from mysql_mimic import MysqlServer
from pathlib import Path
import polars as pl
import pandas as pd

from datetime import date, datetime

# TO allow pyinstaller to find imports
import pyarrow as pa
import xlsxwriter
import numpy as np
import kola
import duckdb
from polars import DataFrame

def exec_with_return(code: str, globals: dict, locals: dict, verbose: bool):
    a = ast.parse(code)
    last_expression = None
    if a.body:
        if isinstance(a_last := a.body[-1], ast.Expr):
            last_expression = ast.unparse(a.body.pop())
        elif isinstance(a_last, ast.Assign):
            last_expression = ast.unparse(a_last.targets[0])
        elif isinstance(a_last, (ast.AnnAssign, ast.AugAssign)):
            last_expression = ast.unparse(a_last.target)
    if verbose:
        print("locals before: ", locals.keys())
    exec(ast.unparse(a), globals, locals)
    if last_expression:
        r = eval(last_expression, globals, locals)
        if verbose:
            print("locals after: ", locals.keys())
        return r


class QueryProcessor:
    def __init__(self, verbose: bool = False, db: duckdb.DuckDBPyConnection = None):
        self.query_lang = "dk"
        if db is None:
            self.duckdb = duckdb.connect(":default:")
        else:
            self.duckdb = db

        self.verbose = verbose
        self.mylocals = {"pythondb":self, "pdb":self}
        self.ctx = pl.SQLContext(register_globals=True, eager=True, frames={})

    def setlang(self, lg:str):
        l = lg.upper()
        if l == "PYTHON":   l = "py"
        elif l == "PY":     l = "py"
        elif l == "DUCKDB": l = "dk"
        elif l == "DK":     l = "dk"
        elif l == "POLARS": l = "pl"
        elif l == "PL": l = "pl"
        else:
                raise Exception("setlang invalid. Must be PY/PL/DK")
        self.query_lang = l

    def getlang(self):
        return self.query_lang

    def getconfig(self):
        return {"lang":self.query_lang}

    def getps1(self):
        return '>>>' if self.query_lang == 'py' else 'q)' if self.query_lang == 'q' else (self.query_lang + ">")

    def queryraw(self, sql):
        s = sql.strip()
        if len(s) == 0:
            return None
        elif s.startswith("pythondb.") or s.startswith("pdb."):  # Always run pythondb as python. Handy to make commands standard?
            s = ">>>" + s
        elif len(s) < 3 or (s[2] != '>' and not s.startswith("q)")): # Add default lang as prefix if none specified
            s = ((self.query_lang + '>') if self.query_lang != 'q' else 'q)') + s
        elif self.query_lang == 'q':
            s = 'q)' + s

        r = None
        if s.startswith("dk>"):
            while s.startswith("dk>"):
                s = s[3:]
            r = self.duckdb.sql(s)
            r = r.pl() if r is not None else None
        elif s.startswith("pl>"):
            while s.startswith("pl>"):
                s = s[3:]
            r = self.ctx.execute(s)
        elif s.startswith(">>>") or s.startswith("py>"):
            while s.startswith(">>>") or s.startswith("py>"):
                s = s[3:]
            r = exec_with_return(s, globals(), self.mylocals, self.verbose)
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
        elif s.startswith("q)"):  # A few Easter eggs
            while s.startswith("q)"):
                s = s[2:]
            if s == '2+2':
                r = 4
            elif s == 'til 10':
                r = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
            elif s == '.z.K':
                r = 5.0
            elif s == '\\\\':
                exit(0)
            else:
                raise Exception("NYI")
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

    def load_files(self, source_files:list[str]) -> None:
        original_lang = self.getlang()
        for srcp in source_files:
            if self.verbose:
                print("Loading file: " + srcp)
            txt = Path(srcp).read_text()
            p = srcp.lower()
            if p.endswith(".sql") and original_lang == 'py':
                self.setlang("dk")
            elif p.endswith(".py"):
                self.setlang("py")
            self.queryraw(txt)
