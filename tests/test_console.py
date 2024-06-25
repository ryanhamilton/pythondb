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

