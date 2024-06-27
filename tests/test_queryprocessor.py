import pytest

from src.hypermodern_python.console import QueryProcessor

tq = ["pdx",
      "2",
      "3.45",
      "complex(6,7)",
      "\"stringy\"",
      "[3,5]",
      '{"a":11,"b":12.5,"c":"st"}',
      "True",
      '{"cherry"}',
      """import pandas as pd
data = [['tom', 10], ['nick', 11], ['juli', 14]]
df = pd.DataFrame(data, columns=['Name', 'Age'])""",
      """import duckdb
duckdb.sql("SELECT 42 AS i")""", ">>>None"]


def chk(a: str, exp: str) -> None:
    assert QueryProcessor().query(a).__str__() == exp


def test_pdx() -> None:
    chk(tq[0], """shape: (2, 2)
┌─────┬─────┐
│ a   ┆ b   │
│ --- ┆ --- │
│ i64 ┆ i64 │
╞═════╪═════╡
│ 1   ┆ 33  │
│ 2   ┆ 41  │
└─────┴─────┘""")


def test_int() -> None:
    chk(tq[1], """shape: (1, 1)
┌─────┐
│ int │
│ --- │
│ i64 │
╞═════╡
│ 2   │
└─────┘""")


def test_float() -> None:
    chk(tq[2], """shape: (1, 1)
┌───────┐
│ float │
│ ---   │
│ f64   │
╞═══════╡
│ 3.45  │
└───────┘""")


def test_complex() -> None:
    chk(tq[3], """shape: (1, 1)
┌─────────┐
│ complex │
│ ---     │
│ object  │
╞═════════╡
│ (6+7j)  │
└─────────┘""")


def test_str() -> None:
    chk(tq[4], """shape: (1, 1)
┌─────────┐
│ str     │
│ ---     │
│ str     │
╞═════════╡
│ stringy │
└─────────┘""")


def test_list() -> None:
    chk(tq[5], """shape: (2, 1)
┌──────┐
│ list │
│ ---  │
│ i64  │
╞══════╡
│ 3    │
│ 5    │
└──────┘""")


def test_dict() -> None:
    chk(tq[6], """shape: (1, 3)
┌─────┬──────┬─────┐
│ a   ┆ b    ┆ c   │
│ --- ┆ ---  ┆ --- │
│ i64 ┆ f64  ┆ str │
╞═════╪══════╪═════╡
│ 11  ┆ 12.5 ┆ st  │
└─────┴──────┴─────┘""")


def test_bool() -> None:
    chk(tq[7], """shape: (1, 1)
┌──────┐
│ bool │
│ ---  │
│ bool │
╞══════╡
│ true │
└──────┘""")


def test_set() -> None:
    chk(tq[8], """shape: (1, 1)
┌────────┐
│ set    │
│ ---    │
│ str    │
╞════════╡
│ cherry │
└────────┘""")


def test_pandas() -> None:
    chk(tq[9], """shape: (3, 2)
┌──────┬─────┐
│ Name ┆ Age │
│ ---  ┆ --- │
│ str  ┆ i64 │
╞══════╪═════╡
│ tom  ┆ 10  │
│ nick ┆ 11  │
│ juli ┆ 14  │
└──────┴─────┘""")


def test_python_duckdb() -> None:
    chk(tq[10], "shape: (1, 1)\n┌─────┐\n│ i   │\n│ --- │\n│ i32 │\n╞═════╡\n│ 42  │\n└─────┘")


def test_dk() -> None:
    chk("dk>select 13 as c", "shape: (1, 1)\n┌─────┐\n│ c   │\n│ --- │\n│ i32 │\n╞═════╡\n│ 13  │\n└─────┘")
    chk("dk>dk>dk>dk>dk>dk>dk>select 13 as c", "shape: (1, 1)\n┌─────┐\n│ c   │\n│ --- │\n│ i32 │\n╞═════╡\n│ 13  │\n└─────┘")


def test_pl() -> None:
    exp = """shape: (2, 2)
┌─────┬─────┐
│ a   ┆ b   │
│ --- ┆ --- │
│ i64 ┆ i64 │
╞═════╪═════╡
│ 1   ┆ 33  │
│ 2   ┆ 41  │
└─────┴─────┘"""
    chk("pl>SELECT * FROM plx;", exp)
    chk("pl>pl>pl>pl>pl>pl>SELECT * FROM plx;", exp)


def test_none() -> None:
    exp = 'shape: (0, 1)\n┌──────┐\n│ None │\n│ ---  │\n│ null │\n╞══════╡\n└──────┘'
    chk(">>>None", exp)
    chk(">>>>>>>>>None", exp)
    chk("py>py>py>py>None", exp)


def test_py() -> None:
    chk("py>2+3", 'shape: (1, 1)\n┌─────┐\n│ int │\n│ --- │\n│ i64 │\n╞═════╡\n│ 5   │\n└─────┘')
    chk(">>>2+3", 'shape: (1, 1)\n┌─────┐\n│ int │\n│ --- │\n│ i64 │\n╞═════╡\n│ 5   │\n└─────┘')


def test_py_created_vars_visible_in_dk_and_pl() -> None:
    runner = QueryProcessor()
    runner.query('py>testboth = pl.DataFrame({"a":[11,33]})')
    exp = """shape: (1, 1)
┌─────┐
│ m   │
│ --- │
│ i64 │
╞═════╡
│ 33  │
└─────┘"""
    assert runner.query("dk>SELECT max(a) AS m FROM testboth").__str__() == exp
    assert runner.query("pl>SELECT max(a) AS m FROM testboth").__str__() == exp