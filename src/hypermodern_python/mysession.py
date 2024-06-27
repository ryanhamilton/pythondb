from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone as timezone_
from functools import partial
from typing import (
    Dict,
    List,
    TYPE_CHECKING,
    Optional,
    Callable,
    Awaitable,
    Type,
    Any,
)

from sqlglot import Dialect
from sqlglot.dialects import MySQL
from sqlglot import expressions as exp
from sqlglot.executor import execute

from mysql_mimic.charset import CharacterSet
from mysql_mimic.errors import ErrorCode, MysqlError
from mysql_mimic.intercept import (
    setitem_kind,
    value_to_expression,
    expression_to_value,
    TRANSACTION_CHARACTERISTICS,
)
from mysql_mimic.schema import (
    show_statement_to_info_schema_query,
    like_to_regex,
    BaseInfoSchema,
    ensure_info_schema,
)
from mysql_mimic.constants import INFO_SCHEMA, KillKind
from mysql_mimic.utils import find_dbs
from mysql_mimic.variables import (
    Variables,
    SessionVariables,
    GlobalVariables,
    DEFAULT,
    parse_timezone,
)
from mysql_mimic.results import AllowedResult

from mysql_mimic import MysqlServer
from src.hypermodern_python.queryprocessor import QueryProcessor
import polars as pl

if TYPE_CHECKING:
    from mysql_mimic.connection import Connection


Middleware = Callable[["Query"], Awaitable[AllowedResult]]


@dataclass
class Query:
    """
    Convenience class that wraps the parameters to middleware.

    Args:
        expression: current query expression
        sql: the original SQL sent by the client
        attrs: query attributes
        _middlewares: subsequent middleware functions
        _query: the ultimate query method
    """

    expression: exp.Expression | None
    sql: str
    attrs: Dict[str, str]
    _middlewares: list[Middleware]
    _query: Callable[[exp.Expression, str, dict[str, str]], Awaitable[AllowedResult]]

    async def next(self) -> AllowedResult:
        """
        Call the next middleware in the chain of middlewares.

        Returns:
            The final query result.
        """
        if not self._middlewares:
            return await self._query(self.expression, self.sql, self.attrs)
        q = Query(
            expression=self.expression,
            sql=self.sql,
            attrs=self.attrs,
            _middlewares=self._middlewares[1:],
            _query=self._query,
        )
        return await self._middlewares[0](q)

    async def start(self) -> AllowedResult:
        """
        Start the middleware chain.

        This should only be called by the framework code
        """
        return await self._middlewares[0](self)


class BaseSession:
    """
    Session interface.

    This defines what the Connection object depends on.

    Most applications to implement the abstract `Session`, not this class.
    """

    variables: Variables
    username: Optional[str]
    database: Optional[str]

    async def handle_query(self, sql: str, attrs: Dict[str, str]) -> AllowedResult:
        """
        Main entrypoint for queries.

        Args:
            sql: SQL statement
            attrs: Mapping of query attributes
        Returns:
            One of:
            - tuple(rows, column_names), where "rows" is a sequence of sequences
              and "column_names" is a sequence of strings with the same length
              as every row in "rows"
            - tuple(rows, result_columns), where "rows" is the same
              as above, and "result_columns" is a sequence of mysql_mimic.ResultColumn
              instances.
            - mysql_mimic.ResultSet instance
        """

    async def init(self, connection: Connection) -> None:
        """
        Called when connection phase is complete.
        """

    async def close(self) -> None:
        """
        Called when the client closes the connection.
        """

    async def reset(self) -> None:
        """
        Called when a client resets the connection and after a COM_CHANGE_USER command.
        """

    async def use(self, database: str) -> None:
        """
        Use a new default database.

        Called when a USE database_name command is received.

        Args:
            database: database name
        """


class Session(BaseSession):
    """
    Abstract session.

    This automatically handles lots of behavior that many clients except,
    e.g. session variables, SHOW commands, queries to INFORMATION_SCHEMA, and more
    """

    dialect: Type[Dialect] = MySQL

    def __init__(self, variables: Variables | None = None):
        self.variables = variables or SessionVariables(GlobalVariables())

        # Query middlewares.
        # These allow queries to be intercepted or wrapped.
        self.middlewares: list[Middleware] = [
            self._set_var_middleware,
            self._replace_variables_middleware,
            self._set_middleware,
            self._static_query_middleware,
            self._use_middleware,
            self._kill_middleware,
            self._show_middleware,
            self._describe_middleware,
            self._begin_middleware,
            self._commit_middleware,
            self._rollback_middleware,
            self._info_schema_middleware,
        ]

        # Information functions.
        # These will be replaced in the AST with their corresponding values.
        self._functions = {
            "CONNECTION_ID": lambda: self.connection.connection_id,
            "USER": lambda: self.variables.get("external_user"),
            "CURRENT_USER": lambda: self.username,
            "VERSION": lambda: self.variables.get("version"),
            "DATABASE": lambda: self.database,
            "NOW": lambda: self.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "CURDATE": lambda: self.timestamp.strftime("%Y-%m-%d"),
            "CURTIME": lambda: self.timestamp.strftime("%H:%M:%S"),
        }
        # Synonyms
        self._functions.update(
            {
                "SYSTEM_USER": self._functions["USER"],
                "SESSION_USER": self._functions["USER"],
                "SCHEMA": self._functions["DATABASE"],
                "CURRENT_TIMESTAMP": self._functions["NOW"],
                "LOCALTIME": self._functions["NOW"],
                "LOCALTIMESTAMP": self._functions["NOW"],
                "CURRENT_DATE": self._functions["CURDATE"],
                "CURRENT_TIME": self._functions["CURTIME"],
            }
        )
        self._constants = {
            "CURRENT_USER",
            "CURRENT_TIME",
            "CURRENT_TIMESTAMP",
            "CURRENT_DATE",
        }

        # Current database
        self.database = None

        # Current authenticated user
        self.username = None

        # Time when query started
        self.timestamp: datetime = datetime.now()

        self._connection: Optional[Connection] = None

    async def query(
        self, expression: exp.Expression, sql: str, attrs: Dict[str, str]
    ) -> AllowedResult:
        """
        Process a SQL query.

        Args:
            expression: parsed AST of the statement from client
            sql: original SQL statement from client
            attrs: arbitrary query attributes set by client
        Returns:
            One of:
            - tuple(rows, column_names), where "rows" is a sequence of sequences
              and "column_names" is a sequence of strings with the same length
              as every row in "rows"
            - tuple(rows, result_columns), where "rows" is the same
              as above, and "result_columns" is a sequence of mysql_mimic.ResultColumn
              instances.
            - mysql_mimic.ResultSet instance
        """
        return [], []

    async def schema(self) -> dict | BaseInfoSchema:
        """
        Provide the database schema.

        This is used to serve INFORMATION_SCHEMA and SHOW queries.

        Returns:
            One of:
            - Mapping of:
                {table: {column: column_type}} or
                {db: {table: {column: column_type}}} or
                {catalog: {db: {table: {column: column_type}}}}
            - Instance of `BaseInfoSchema`
        """
        return {}

    @property
    def connection(self) -> Connection:
        """
        Get the connection associated with this session.
        """
        if self._connection is None:
            raise AttributeError("Session is not yet bound")
        return self._connection

    async def init(self, connection: Connection) -> None:
        """
        Called when connection phase is complete.
        """
        self._connection = connection

    async def close(self) -> None:
        """
        Called when the client closes the connection.
        """
        self._connection = None

    async def handle_query(self, sql: str, attrs: Dict[str, str]) -> AllowedResult:
        self.timestamp = datetime.now(tz=self.timezone())
        result = None

        # Ryan - Hack to try parsing and normally run that but then revert to standard run
        exps = None
        try :
            exps = self._parse(sql)
        except:
            exps = None

        if exps is None:
            q = Query(expression=None,sql=sql,attrs=attrs,_middlewares=self.middlewares,_query=self.query,)
            result = await q.start()
        else:
            for expression in exps:
                if not expression:
                    continue
                q = Query(
                    expression=expression,
                    sql=sql,
                    attrs=attrs,
                    _middlewares=self.middlewares,
                    _query=self.query,
                )
                result = await q.start()
        return result

    async def use(self, database: str) -> None:
        self.database = database

    def _parse(self, sql: str) -> List[exp.Expression]:
        return [e for e in self.dialect().parse(sql) if e]

    async def _query_info_schema(self, expression: exp.Expression) -> AllowedResult:
        return await ensure_info_schema(await self.schema()).query(expression)

    async def _set_var_middleware(self, q: Query) -> AllowedResult:
        if q.expression is None: # Ryan - Hack as this shouldn't really be None
            return await q.next()
        """Handles any SET_VAR hints, which set system variables for a single statement"""
        hints = q.expression.find_all(exp.Hint)
        if not hints:
            return await q.next()

        assignments = {}

        # Iterate in reverse order so higher SET_VAR hints get priority
        for hint in reversed(list(hints)):
            set_var_hint = None

            for e in hint.expressions:
                if isinstance(e, exp.Func) and e.name == "SET_VAR":
                    set_var_hint = e
                    for eq in e.expressions:
                        assignments[eq.left.name] = expression_to_value(eq.right)

            if set_var_hint:
                set_var_hint.pop()

            # Remove the hint entirely if SET_VAR was the only expression
            if not hint.expressions:
                hint.pop()

        orig = {k: self.variables.get(k) for k in assignments}
        try:
            for k, v in assignments.items():
                self.variables.set(k, v)
            return await q.next()
        finally:
            for k, v in orig.items():
                self.variables.set(k, v)

    async def _use_middleware(self, q: Query) -> AllowedResult:
        """Intercept USE statements"""
        if isinstance(q.expression, exp.Use):
            await self.use(q.expression.this.name)
            return [], []
        return await q.next()

    async def _kill_middleware(self, q: Query) -> AllowedResult:
        """Intercept KILL statements"""
        if isinstance(q.expression, exp.Kill):
            control = self.connection.control
            if control:
                kind = KillKind[q.expression.text("kind").upper() or "CONNECTION"]
                this = q.expression.this.name

                try:
                    connection_id = int(this)
                except ValueError as e:
                    raise MysqlError(
                        f"Invalid KILL connection ID: {this}",
                        code=ErrorCode.PARSE_ERROR,
                    ) from e

                await control.kill(connection_id, kind)
            return [], []
        return await q.next()

    async def _show_middleware(self, q: Query) -> AllowedResult:
        """Intercept SHOW statements"""
        if isinstance(q.expression, exp.Show):
            return await self._show(q.expression)
        return await q.next()

    async def _show(self, expression: exp.Show) -> AllowedResult:
        kind = expression.name.upper()
        if kind == "VARIABLES":
            return self._show_variables(expression)
        if kind == "STATUS":
            return self._show_status(expression)
        if kind == "WARNINGS":
            return self._show_warnings(expression)
        if kind == "ERRORS":
            return self._show_errors(expression)
        select = show_statement_to_info_schema_query(expression, self.database)
        return await self._query_info_schema(select)

    async def _describe_middleware(self, q: Query) -> AllowedResult:
        """Intercept DESCRIBE statements"""
        if isinstance(q.expression, exp.Describe):
            name = q.expression.this.name
            show = self.dialect().parse(f"SHOW COLUMNS FROM {name}")[0]
            return await self._show(show) if isinstance(show, exp.Show) else None
        return await q.next()

    async def _rollback_middleware(self, q: Query) -> AllowedResult:
        """Intercept ROLLBACK statements"""
        if isinstance(q.expression, exp.Rollback):
            return [], []
        return await q.next()

    async def _commit_middleware(self, q: Query) -> AllowedResult:
        """Intercept COMMIT statements"""
        if isinstance(q.expression, exp.Commit):
            return [], []
        return await q.next()

    async def _begin_middleware(self, q: Query) -> AllowedResult:
        """Intercept BEGIN statements"""
        if isinstance(q.expression, exp.Transaction):
            return [], []
        return await q.next()

    async def _replace_variables_middleware(self, q: Query) -> AllowedResult:
        """Replace session variables and information functions with their corresponding values"""

        def _transform(node: exp.Expression) -> exp.Expression:
            new_node = None

            if isinstance(node, exp.Func):
                if isinstance(node, exp.Anonymous):
                    func_name = node.name.upper()
                else:
                    func_name = node.sql_name()
                func = self._functions.get(func_name)
                if func:
                    value = func()
                    new_node = value_to_expression(value)
            elif isinstance(node, exp.Column) and node.sql() in self._constants:
                value = self._functions[node.sql()]()
                new_node = value_to_expression(value)
            elif isinstance(node, exp.SessionParameter):
                value = self.variables.get(node.name)
                new_node = value_to_expression(value)

            if (
                new_node
                and isinstance(node.parent, exp.Select)
                and node.arg_key == "expressions"
            ):
                new_node = exp.alias_(new_node, exp.to_identifier(node.sql()))

            return new_node or node

        if isinstance(q.expression, exp.Set):
            for setitem in q.expression.expressions:
                if isinstance(setitem.this, exp.Binary):
                    # In the case of statements like: SET @@foo = @@bar
                    # We only want to replace variables on the right
                    setitem.this.set(
                        "expression",
                        setitem.this.expression.transform(_transform, copy=True),
                    )
        elif q.expression is not None:
            q.expression.transform(_transform, copy=False)

        return await q.next()

    async def _static_query_middleware(self, q: Query) -> AllowedResult:
        """
        Handle static queries (e.g. SELECT 1).

        These very common, as many clients execute commands like SELECT DATABASE() when connecting.
        """
        if isinstance(q.expression, exp.Select) and not any(
            q.expression.args.get(a)
            for a in set(exp.Select.arg_types) - {"expressions", "limit", "hint"}
        ):
            result = execute(q.expression)
            return result.rows, result.columns
        return await q.next()

    async def _set_middleware(self, q: Query) -> AllowedResult:
        """Intercept SET statements"""
        if isinstance(q.expression, exp.Set):
            expressions = q.expression.expressions
            for item in expressions:
                assert isinstance(item, exp.SetItem)

                kind = setitem_kind(item)

                if kind == "VARIABLE":
                    self._set_variable(item)
                elif kind == "CHARACTER SET":
                    self._set_charset(item)
                elif kind == "NAMES":
                    self._set_names(item)
                elif kind == "TRANSACTION":
                    self._set_transaction(item)
                else:
                    raise MysqlError(
                        f"Unsupported SET statement: {kind}",
                        code=ErrorCode.NOT_SUPPORTED_YET,
                    )

            return [], []
        return await q.next()

    async def _info_schema_middleware(self, q: Query) -> AllowedResult:
        """Intercept queries to INFORMATION_SCHEMA tables"""
        dbs = find_dbs(q.expression)
        if (self.database and self.database.lower() in INFO_SCHEMA) or (
            dbs and all(db.lower() in INFO_SCHEMA for db in dbs)
        ):
            return await self._query_info_schema(q.expression)
        return await q.next()

    def _set_variable(self, setitem: exp.SetItem) -> None:
        assignment = setitem.this
        left = assignment.left

        if isinstance(left, exp.SessionParameter):
            scope = left.text("kind") or "SESSION"
            name = left.name
        elif isinstance(left, exp.Parameter):
            raise MysqlError(
                "User-defined variables not supported yet",
                code=ErrorCode.NOT_SUPPORTED_YET,
            )
        else:
            scope = setitem.text("kind") or "SESSION"
            name = left.name

        scope = scope.upper()
        value = expression_to_value(assignment.right)

        if scope in {"SESSION", "LOCAL"}:
            self.variables.set(name, value)
        else:
            raise MysqlError(
                f"Cannot SET variable {name} with scope {scope}",
                code=ErrorCode.NOT_SUPPORTED_YET,
            )

    def _set_charset(self, item: exp.SetItem) -> None:
        charset_name: Any
        charset_conn: Any
        if item.name == "DEFAULT":
            charset_name = DEFAULT
            charset_conn = DEFAULT
        else:
            charset_name = item.name
            charset_conn = self.variables.get("character_set_database")
        self.variables.set("character_set_client", charset_name)
        self.variables.set("character_set_results", charset_name)
        self.variables.set("character_set_connection", charset_conn)

    def _set_names(self, item: exp.SetItem) -> None:
        charset_name: Any
        collation_name: Any
        if item.name == "DEFAULT":
            charset_name = DEFAULT
            collation_name = DEFAULT
        else:
            charset_name = item.name
            collation_name = (
                item.text("collate")
                or CharacterSet[charset_name].default_collation.name
            )
        self.variables.set("character_set_client", charset_name)
        self.variables.set("character_set_connection", charset_name)
        self.variables.set("character_set_results", charset_name)
        self.variables.set("collation_connection", collation_name)

    def _set_transaction(self, item: exp.SetItem) -> None:
        characteristics = [e.name.upper() for e in item.expressions]
        for characteristic in characteristics:
            variable, value = TRANSACTION_CHARACTERISTICS[characteristic]
            self.variables.set(variable, value)

    def _show_variables(self, show: exp.Show) -> AllowedResult:
        rows = [(k, None if v is None else str(v)) for k, v in self.variables.list()]
        like = show.text("like")
        if like:
            rows = [(k, v) for k, v in rows if like_to_regex(like).match(k)]
        return rows, ["Variable_name", "Value"]

    def _show_status(self, show: exp.Show) -> AllowedResult:
        return [], ["Variable_name", "Value"]

    def _show_warnings(self, show: exp.Show) -> AllowedResult:
        return [], ["Level", "Code", "Message"]

    def _show_errors(self, show: exp.Show) -> AllowedResult:
        return [], ["Level", "Code", "Message"]

    def timezone(self) -> timezone_:
        tz = self.variables.get("time_zone")
        return parse_timezone(tz)


class MySession(Session):
    def __init__(self, queryProcessor: QueryProcessor):
        super().__init__()
        self.queryProcessor = queryProcessor

    # 22
    # 3.3
    # complex(5, 3)
    # "ab"
    # [2, 3]
    # ("pp", 22)
    # range(4)
    # {"a": 11, "b": 12}
    # True
    # {"apple", "banana", "cherry"}

    async def query(self, expression, sql, attrs):
        r = self.queryProcessor.query(sql)
        if isinstance(r, pl.DataFrame):
            return r.rows(), r.columns

        return [("a", 1), ("b", 2)], ["col1", "col2"]

    async def schema(self):
        # Optionally provide the database schema.
        # This is used to serve INFORMATION_SCHEMA and SHOW queries.
        tbls = self.queryProcessor.query("dk>show tables;")
        r = {}
        for c in tbls.get_column("name"):
            r[c] = {"col1": "TEXT", "col2": "INT", }
        return r

def start_sql(queryProcessor: QueryProcessor, port: int):
    print("Starting MySQL Server port: ", port)
    handler = partial(MySession, queryProcessor)
    server = MysqlServer(session_factory=handler, port=port)
    asyncio.run(server.serve_forever())

