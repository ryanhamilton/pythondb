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
