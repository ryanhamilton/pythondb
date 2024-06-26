import pytest

from src.hypermodern_python.console import QueryProcessor


@pytest.fixture
def runner() -> QueryProcessor:
    return QueryProcessor()


def test_pdx(runner: QueryProcessor) -> None:
    assert runner.query("pdx").__str__() == """shape: (2, 2)
┌─────┬─────┐
│ a   ┆ b   │
│ --- ┆ --- │
│ i64 ┆ i64 │
╞═════╪═════╡
│ 1   ┆ 33  │
│ 2   ┆ 41  │
└─────┴─────┘"""


def test_int(runner: QueryProcessor) -> None:
    assert runner.query("2").__str__() == """shape: (1, 1)
┌─────┐
│ int │
│ --- │
│ i64 │
╞═════╡
│ 2   │
└─────┘"""


def test_float(runner: QueryProcessor) -> None:
    assert runner.query("3.45").__str__() == """shape: (1, 1)
┌───────┐
│ float │
│ ---   │
│ f64   │
╞═══════╡
│ 3.45  │
└───────┘"""


def test_complex(runner: QueryProcessor) -> None:
    assert runner.query("complex(6,7)").__str__() == """shape: (1, 1)
┌─────────┐
│ complex │
│ ---     │
│ object  │
╞═════════╡
│ (6+7j)  │
└─────────┘"""


def test_str(runner: QueryProcessor) -> None:
    assert runner.query("\"stringy\"").__str__() == """shape: (1, 1)
┌─────────┐
│ str     │
│ ---     │
│ str     │
╞═════════╡
│ stringy │
└─────────┘"""


def test_list(runner: QueryProcessor) -> None:
    assert runner.query("[3,5]").__str__() == """shape: (2, 1)
┌──────┐
│ list │
│ ---  │
│ i64  │
╞══════╡
│ 3    │
│ 5    │
└──────┘"""


def test_dict(runner: QueryProcessor) -> None:
    assert runner.query('{"a":11,"b":12.5,"c":"st"}').__str__() == """shape: (1, 3)
┌─────┬──────┬─────┐
│ a   ┆ b    ┆ c   │
│ --- ┆ ---  ┆ --- │
│ i64 ┆ f64  ┆ str │
╞═════╪══════╪═════╡
│ 11  ┆ 12.5 ┆ st  │
└─────┴──────┴─────┘"""


def test_bool(runner: QueryProcessor) -> None:
    assert runner.query("True").__str__() == """shape: (1, 1)
┌──────┐
│ bool │
│ ---  │
│ bool │
╞══════╡
│ true │
└──────┘"""


def test_set(runner: QueryProcessor) -> None:
    assert runner.query('{"cherry"}').__str__() == """shape: (1, 1)
┌────────┐
│ set    │
│ ---    │
│ str    │
╞════════╡
│ cherry │
└────────┘"""


def test_pandas(runner: QueryProcessor) -> None:
    qry = """import pandas as pd
data = [['tom', 10], ['nick', 11], ['juli', 14]]
df = pd.DataFrame(data, columns=['Name', 'Age'])"""
    assert runner.query(qry).__str__() == """shape: (3, 2)
┌──────┬─────┐
│ Name ┆ Age │
│ ---  ┆ --- │
│ str  ┆ i64 │
╞══════╪═════╡
│ tom  ┆ 10  │
│ nick ┆ 11  │
│ juli ┆ 14  │
└──────┴─────┘"""

def test_python_duckdb(runner: QueryProcessor) -> None:
    qry = """import duckdb
duckdb.sql("SELECT 42 AS i")"""
    assert runner.query(qry).__str__() == "shape: (1, 1)\n┌─────┐\n│ i   │\n│ --- │\n│ i32 │\n╞═════╡\n│ 42  │\n└─────┘"


def test_dk(runner: QueryProcessor) -> None:
    assert runner.query("dk>select 13 as c").__str__() == "shape: (1, 1)\n┌─────┐\n│ c   │\n│ --- │\n│ i32 │\n╞═════╡\n│ 13  │\n└─────┘"
    assert runner.query("dk>dk>dk>dk>dk>dk>dk>select 13 as c").__str__() == "shape: (1, 1)\n┌─────┐\n│ c   │\n│ --- │\n│ i32 │\n╞═════╡\n│ 13  │\n└─────┘"


def test_pl(runner: QueryProcessor) -> None:
    exp = """shape: (2, 2)
┌─────┬─────┐
│ a   ┆ b   │
│ --- ┆ --- │
│ i64 ┆ i64 │
╞═════╪═════╡
│ 1   ┆ 33  │
│ 2   ┆ 41  │
└─────┴─────┘"""
    assert runner.query("pl>SELECT * FROM plx;").__str__() == exp
    assert runner.query("pl>pl>pl>pl>pl>pl>SELECT * FROM plx;").__str__() == exp


def test_none(runner: QueryProcessor) -> None:
    exp = 'shape: (0, 1)\n┌──────┐\n│ None │\n│ ---  │\n│ null │\n╞══════╡\n└──────┘'
    assert runner.query(">>>None").__str__() == exp
    assert runner.query(">>>>>>>>>None").__str__() == exp
    assert runner.query("py>py>py>py>None").__str__() == exp


def test_py(runner: QueryProcessor) -> None:
    assert runner.query("py>2+3").__str__() == 'shape: (1, 1)\n┌─────┐\n│ int │\n│ --- │\n│ i64 │\n╞═════╡\n│ 5   │\n└─────┘'
    assert runner.query(">>>2+3").__str__() == 'shape: (1, 1)\n┌─────┐\n│ int │\n│ --- │\n│ i64 │\n╞═════╡\n│ 5   │\n└─────┘'


def test_py_created_vars_visible_in_dk_and_pl(runner: QueryProcessor) -> None:
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