from copy import copy
from functools import reduce
from typing import (
    Optional,
    Any,
    Union,
    Type,
    Sequence,
    List,
    Tuple as TypedTuple,
)

from .aliased_query import AliasedQuery
from pypika.enums import JoinType, SetOperation, Dialects
from .node import Node
from .selectable import Selectable
from pypika.terms import (
    ArithmeticExpression,
    EmptyCriterion,
    Field,
    Function,
    Index,
    Rollup,
    Star,
    Term,
    Tuple,
    ValueWrapper,
    Criterion,
)
from pypika.utils import (
    JoinException,
    QueryException,
    RollupException,
    SetOperationException,
    copy_if_immutable,
    format_alias_sql,
    format_quotes,
    ignore_copy,
)

__author__ = "Timothy Heys"
__email__ = "theys@kayak.com"


class Schema:
    def __init__(self, name: str, parent: Optional["Schema"] = None) -> None:
        self._name = name
        self._parent = parent

    def __eq__(self, other: "Schema") -> bool:
        return (
            isinstance(other, Schema)
            and self._name == other._name
            and self._parent == other._parent
        )

    def __ne__(self, other: "Schema") -> bool:
        return not self.__eq__(other)

    @ignore_copy
    def __getattr__(self, item: str) -> "Table":
        return Table(item, schema=self)

    def get_sql(self, quote_char: Optional[str] = None, **kwargs: Any) -> str:
        # FIXME escape
        schema_sql = format_quotes(self._name, quote_char)

        if self._parent is not None:
            return "{parent}.{schema}".format(
                parent=self._parent.get_sql(quote_char=quote_char, **kwargs),
                schema=schema_sql,
            )

        return schema_sql


class Database(Schema):
    @ignore_copy
    def __getattr__(self, item: str) -> Schema:
        return Schema(item, parent=self)


class Table(Selectable):
    @staticmethod
    def _init_schema(schema: Union[str, list, tuple, Schema, None]) -> Union[str, list, tuple, Schema, None]:
        # This is a bit complicated in order to support backwards compatibility. It should probably be cleaned up for
        # the next major release. Schema is accepted as a string, list/tuple, Schema instance, or None
        if isinstance(schema, Schema):
            return schema
        if isinstance(schema, (list, tuple)):
            return reduce(
                lambda obj, s: Schema(s, parent=obj), schema[1:], Schema(schema[0])
            )
        if schema is not None:
            return Schema(schema)
        return None

    def __init__(self, name: str, schema: Optional[Union[Schema, str]] = None, alias: Optional[str] = None, query_cls: Optional[Type["Query"]] = None) -> None:
        super().__init__(alias)
        self._table_name = name
        self._schema = self._init_schema(schema)
        self._query_cls = query_cls or Query
        if not issubclass(self._query_cls, Query):
            raise TypeError("Expected 'query_cls' to be subclass of Query")

    def get_table_name(self) -> str:
        return self.alias or self._table_name

    def get_sql(self, **kwargs: Any) -> str:
        quote_char = kwargs.get("quote_char")
        # FIXME escape
        table_sql = format_quotes(self._table_name, quote_char)

        if self._schema is not None:
            table_sql = "{schema}.{table}".format(
                schema=self._schema.get_sql(**kwargs), table=table_sql
            )

        return format_alias_sql(table_sql, self.alias, **kwargs)

    def __str__(self) -> str:
        return self.get_sql(quote_char='"')

    def __eq__(self, other) -> bool:
        if not isinstance(other, Table):
            return False

        if self._table_name != other._table_name:
            return False

        if self._schema != other._schema:
            return False

        if self.alias != other.alias:
            return False

        return True

    def __repr__(self) -> str:
        if self._schema:
            return "Table('{}', schema='{}')".format(self._table_name, self._schema)
        return "Table('{}')".format(self._table_name)

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash(str(self))

    def select(self, *terms: Sequence[Union[int, float, str, bool, Term, Field]]) -> "QueryBuilder":
        """
        Perform a SELECT operation on the current table

        :param terms:
            Type:  list[expression]

            A list of terms to select. These can be any type of int, float, str, bool or Term or a Field.

        :return:  QueryBuilder
        """
        return self._query_cls.from_(self).select(*terms)

    def update(self) -> "QueryBuilder":
        """
        Perform an UPDATE operation on the current table

        :return: QueryBuilder
        """
        return self._query_cls.update(self)

    def insert(self, *terms: Union[int, float, str, bool, Term, Field]) -> "QueryBuilder":
        """
        Perform an INSERT operation on the current table

        :param terms:
            Type: list[expression]

            A list of terms to select. These can be any type of int, float, str, bool or  any other valid data

        :return: QueryBuilder
        """
        return self._query_cls.into(self).insert(*terms)


def make_tables(*names: Union[TypedTuple[str, str], str], **kwargs: Any) -> List[Table]:
    """
    Shortcut to create many tables. If `names` param is a tuple, the first
    position will refer to the `_table_name` while the second will be its `alias`.
    Any other data structure will be treated as a whole as the `_table_name`.
    """
    tables = []
    for name in names:
        if isinstance(name, tuple) and len(name) == 2:
            t = Table(
                name=name[0], alias=name[1], schema=kwargs.get("schema"),
                query_cls=kwargs.get("query_cls"),
            )
        else:
            t = Table(
                name=name, schema=kwargs.get("schema"),
                query_cls=kwargs.get("query_cls"),
            )
        tables.append(t)
    return tables


class Column:
    """Represents a column.
    """

    def __init__(
            self,
            column_name: str,
            column_type: Optional[str] = None,
            nullable: Optional[bool] = None,
            default: Optional[Term] = None
    ) -> None:
        self.name = column_name
        self.type = column_type
        self.nullable = nullable
        self.default = default

    def get_name_sql(self, **kwargs: Any) -> str:
        quote_char = kwargs.get("quote_char")

        column_sql = "{name}".format(
            name=format_quotes(self.name, quote_char),
        )

        return column_sql

    def get_sql(self, **kwargs: Any) -> str:
        column_sql = "{name}{type}{nullable}{default}".format(
            name=self.get_name_sql(**kwargs),
            type=" {}".format(self.type) if self.type else "",
            nullable=" {}".format("NULL" if self.nullable else "NOT NULL") if self.nullable is not None else "",
            default=" {}".format("DEFAULT " + self.default.get_sql(**kwargs)) if self.default else "",
        )

        return column_sql

    def __str__(self) -> str:
        return self.get_sql(quote_char='"')


def make_columns(*names: Union[TypedTuple[str, str], str]) -> List[Column]:
    """
    Shortcut to create many columns. If `names` param is a tuple, the first
    position will refer to the `name` while the second will be its `type`.
    Any other data structure will be treated as a whole as the `name`.
    """
    columns = []
    for name in names:
        if isinstance(name, tuple) and len(name) == 2:
            column = Column(column_name=name[0], column_type=name[1])
        else:
            column = Column(column_name=name)
        columns.append(column)

    return columns


class PeriodFor:
    def __init__(self, name: str, start_column: Union[str, Column], end_column: Union[str, Column]) -> None:
        self.name = name
        self.start_column = start_column if isinstance(start_column, Column) else Column(start_column)
        self.end_column = end_column if isinstance(end_column, Column) else Column(end_column)

    def get_sql(self, **kwargs: Any) -> str:
        quote_char = kwargs.get("quote_char")

        period_for_sql = "PERIOD FOR {name} ({start_column_name},{end_column_name})".format(
            name=format_quotes(self.name, quote_char),
            start_column_name=self.start_column.get_name_sql(**kwargs),
            end_column_name=self.end_column.get_name_sql(**kwargs)
        )

        return period_for_sql


# for typing in Query's methods
_TableClass = Table


class Query:
    """
    Query is the primary class and entry point in pypika. It is used to build queries iteratively using the builder
    design
    pattern.

    This class is immutable.
    """

    @classmethod
    def _builder(cls, **kwargs: Any) -> "QueryBuilder":
        return QueryBuilder(**kwargs)

    @classmethod
    def from_(cls, table: Union[Selectable, str], **kwargs: Any) -> "QueryBuilder":
        """
        Query builder entry point.  Initializes query building and sets the table to select from.  When using this
        function, the query becomes a SELECT query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return cls._builder(**kwargs).from_(table)

    @classmethod
    def create_table(cls, table: Union[str, Table]) -> "CreateQueryBuilder":
        """
        Query builder entry point. Initializes query building and sets the table name to be created. When using this
        function, the query becomes a CREATE statement.

        :param table: An instance of a Table object or a string table name.

        :return: CreateQueryBuilder
        """
        return CreateQueryBuilder().create_table(table)

    @classmethod
    def into(cls, table: Union[Table, str], **kwargs: Any) -> "QueryBuilder":
        """
        Query builder entry point.  Initializes query building and sets the table to insert into.  When using this
        function, the query becomes an INSERT query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return cls._builder(**kwargs).into(table)

    @classmethod
    def with_(cls, table: Union[str, Selectable], name: str, **kwargs: Any) -> "QueryBuilder":
        return cls._builder(**kwargs).with_(table, name)

    @classmethod
    def select(cls, *terms: Union[int, float, str, bool, Term], **kwargs: Any) -> "QueryBuilder":
        """
        Query builder entry point.  Initializes query building without a table and selects fields.  Useful when testing
        SQL functions.

        :param terms:
            Type: list[expression]

            A list of terms to select.  These can be any type of int, float, str, bool, or Term.  They cannot be a Field
            unless the function ``Query.from_`` is called first.

        :returns QueryBuilder
        """
        return cls._builder(**kwargs).select(*terms)

    @classmethod
    def update(cls, table: Union[str, Table], **kwargs) -> "QueryBuilder":
        """
        Query builder entry point.  Initializes query building and sets the table to update.  When using this
        function, the query becomes an UPDATE query.

        :param table:
            Type: Table or str

            An instance of a Table object or a string table name.

        :returns QueryBuilder
        """
        return cls._builder(**kwargs).update(table)

    @classmethod
    def Table(cls, table_name: str, **kwargs) -> _TableClass:
        """
        Convenience method for creating a Table that uses this Query class.

        :param table_name:
            Type: str

            A string table name.

        :returns Table
        """
        kwargs["query_cls"] = cls
        return Table(table_name, **kwargs)

    @classmethod
    def Tables(cls, *names: Union[TypedTuple[str, str], str], **kwargs: Any) -> List[_TableClass]:
        """
        Convenience method for creating many tables that uses this Query class.
        See ``Query.make_tables`` for details.

        :param names:
            Type: list[str or tuple]

            A list of string table names, or name and alias tuples.

        :returns Table
        """
        kwargs["query_cls"] = cls
        return make_tables(*names, **kwargs)


class _SetOperation(Selectable, Term):
    """
    A Query class wrapper for a all set operations, Union DISTINCT or ALL, Intersect, Except or Minus

    Created via the functions `Query.union`,`Query.union_all`,`Query.intersect`, `Query.except_of`,`Query.minus`.

    This class should not be instantiated directly.
    """

    def __init__(
        self, base_query: "QueryBuilder", set_operation_query: "QueryBuilder", set_operation: SetOperation, alias: Optional[str] = None, wrapper_cls: Type[ValueWrapper] = ValueWrapper,
    ):
        super().__init__(alias)
        self.base_query = base_query
        self._set_operation = [(set_operation, set_operation_query)]
        self._orderbys = []

        self._limit = None
        self._offset = None

        self._wrapper_cls = wrapper_cls

    def orderby(self, *fields: Field, **kwargs: Any) -> "_SetOperation":
        with copy_if_immutable(self) as this:
            for field in fields:
                field = (
                    Field(field, table=this.base_query._from[0])
                    if isinstance(field, str)
                    else this.base_query.wrap_constant(field)
                )

                this._orderbys.append((field, kwargs.get("order")))
                return this

    def limit(self, limit: int) -> "_SetOperation":
        with copy_if_immutable(self) as this:
            this._limit = limit
            return this

    def offset(self, offset: int) -> "_SetOperation":
        with copy_if_immutable(self) as this:
            this._offset = offset
            return this

    def union(self, other: Selectable) -> "_SetOperation":
        with copy_if_immutable(self) as this:
            this._set_operation.append((SetOperation.union, other))
            return this

    def union_all(self, other: Selectable) -> "_SetOperation":
        with copy_if_immutable(self) as this:
            this._set_operation.append((SetOperation.union_all, other))
            return this

    def intersect(self, other: Selectable) -> "_SetOperation":
        with copy_if_immutable(self) as this:
            this._set_operation.append((SetOperation.intersect, other))
            return this

    def except_of(self, other: Selectable) -> "_SetOperation":
        with copy_if_immutable(self) as this:
            this._set_operation.append((SetOperation.except_of, other))
            return this

    def minus(self, other: Selectable) -> "_SetOperation":
        with copy_if_immutable(self) as this:
            this._set_operation.append((SetOperation.minus, other))
            return this

    def __add__(self, other: Selectable) -> "_SetOperation":
        return self.union(other)

    def __mul__(self, other: Selectable) -> "_SetOperation":
        return self.union_all(other)

    def __sub__(self, other: "QueryBuilder") -> "_SetOperation":
        return self.minus(other)

    def __str__(self) -> str:
        return self.get_sql()

    def get_sql(self, with_alias: bool = False, subquery: bool = False, **kwargs: Any) -> str:
        set_operation_template = " {type} {query_string}"

        kwargs.setdefault("dialect", self.base_query.dialect)
        # This initializes the quote char based on the base query, which could be a dialect specific query class
        # This might be overridden if quote_char is set explicitly in kwargs
        kwargs.setdefault("quote_char", self.base_query.QUOTE_CHAR)

        base_querystring = self.base_query.get_sql(
            subquery=self.base_query.wrap_set_operation_queries, **kwargs
        )

        querystring = base_querystring
        for set_operation, set_operation_query in self._set_operation:
            set_operation_querystring = set_operation_query.get_sql(
                subquery=self.base_query.wrap_set_operation_queries, **kwargs
            )

            if len(self.base_query._selects) != len(set_operation_query._selects):
                raise SetOperationException(
                    "Queries must have an equal number of select statements in a set operation."
                    "\n\nMain Query:\n{query1}\n\nSet Operations Query:\n{query2}".format(
                        query1=base_querystring, query2=set_operation_querystring
                    )
                )

            querystring += set_operation_template.format(
                type=set_operation.value, query_string=set_operation_querystring
            )

        if self._orderbys:
            querystring += self._orderby_sql(**kwargs)

        if self._limit is not None:
            querystring += self._limit_sql()

        if self._offset:
            querystring += self._offset_sql()

        if subquery:
            querystring = "({query})".format(query=querystring, **kwargs)

        if with_alias:
            return format_alias_sql(
                querystring, self.alias or self._table_name, **kwargs
            )

        return querystring

    def _orderby_sql(self, quote_char: Optional[str] = None, **kwargs: Any) -> str:
        """
        Produces the ORDER BY part of the query.  This is a list of fields and possibly their directionality, ASC or
        DESC. The clauses are stored in the query under self._orderbys as a list of tuples containing the field and
        directionality (which can be None).

        If an order by field is used in the select clause, determined by a matching , then the ORDER BY clause will use
        the alias, otherwise the field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self.base_query._selects}
        for field, directionality in self._orderbys:
            term = (
                format_quotes(field.alias, quote_char)
                if field.alias and field.alias in selected_aliases
                else field.get_sql(quote_char=quote_char, **kwargs)
            )

            clauses.append(
                "{term} {orient}".format(term=term, orient=directionality.value)
                if directionality is not None
                else term
            )

        return " ORDER BY {orderby}".format(orderby=",".join(clauses))

    def _offset_sql(self) -> str:
        return " OFFSET {offset}".format(offset=self._offset)

    def _limit_sql(self) -> str:
        return " LIMIT {limit}".format(limit=self._limit)


class QueryBuilder(Selectable, Term):
    """
    Query Builder is the main class in pypika which stores the state of a query and offers functions which allow the
    state to be branched immutably.
    """

    QUOTE_CHAR: Optional[str] = '"'
    SECONDARY_QUOTE_CHAR: Optional[str] = "'"
    ALIAS_QUOTE_CHAR: Optional[str] = None
    QUERY_ALIAS_QUOTE_CHAR: Optional[str] = None

    def __init__(
        self,
        dialect: Optional[Dialects] = None,
        wrap_set_operation_queries: bool = True,
        wrapper_cls: Type[ValueWrapper] = ValueWrapper,
        immutable: bool = True,
        as_keyword: bool = False,
    ):
        super().__init__(None)

        self._from = []
        self._insert_table = None
        self._update_table = None
        self._delete_from = False
        self._replace = False

        self._with = []
        self._selects = []
        self._force_indexes = []
        self._use_indexes = []
        self._columns = []
        self._values = []
        self._distinct = False
        self._ignore = False
        self._for_update = False

        self._wheres = None
        self._prewheres = None
        self._groupbys = []
        self._with_totals = False
        self._havings = None
        self._orderbys = []
        self._joins = []
        self._unions = []

        self._limit = None
        self._offset = None

        self._updates = []

        self._select_star = False
        self._select_star_tables = set()
        self._mysql_rollup = False
        self._select_into = False

        self._subquery_count = 0
        self._foreign_table = False

        self.dialect = dialect
        self.as_keyword = as_keyword
        self.wrap_set_operation_queries = wrap_set_operation_queries

        self._wrapper_cls = wrapper_cls

        self.immutable = immutable

    def __copy__(self) -> "QueryBuilder":
        newone = type(self).__new__(type(self))
        newone.__dict__.update(self.__dict__)
        newone._select_star_tables = copy(self._select_star_tables)
        newone._from = copy(self._from)
        newone._with = copy(self._with)
        newone._selects = copy(self._selects)
        newone._columns = copy(self._columns)
        newone._values = copy(self._values)
        newone._groupbys = copy(self._groupbys)
        newone._orderbys = copy(self._orderbys)
        newone._joins = copy(self._joins)
        newone._unions = copy(self._unions)
        newone._updates = copy(self._updates)
        return newone

    def from_(self, selectable: Union[Selectable, Query, str]) -> "QueryBuilder":
        """
        Adds a table to the query. This function can only be called once and will raise an AttributeError if called a
        second time.

        :param selectable:
            Type: ``Table``, ``Query``, or ``str``

            When a ``str`` is passed, a table with the name matching the ``str`` value is used.

        :returns
            A copy of the query with the table added.
        """
        with copy_if_immutable(self) as this:
            this._from.append(
                Table(selectable) if isinstance(selectable, str) else selectable
            )

            if (
                isinstance(selectable, (QueryBuilder, _SetOperation))
                and selectable.alias is None
            ):
                if isinstance(selectable, QueryBuilder):
                    sub_query_count = selectable._subquery_count
                else:
                    sub_query_count = 0

                sub_query_count = max(this._subquery_count, sub_query_count)
                selectable.alias = "sq%d" % sub_query_count
                this._subquery_count = sub_query_count + 1
            return this

    def replace_table(self, current_table: Optional[Table], new_table: Optional[Table]) -> "QueryBuilder":
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing fields across
        queries.

        :param current_table:
            The table instance to be replaces.
        :param new_table:
            The table instance to replace with.
        :return:
            A copy of the query with the tables replaced.
        """
        with copy_if_immutable(self) as this:
            this._from = [
                new_table if table == current_table else table for table in this._from
            ]
            this._insert_table = new_table if this._insert_table else None
            this._update_table = new_table if this._update_table else None

            this._with = [
                alias_query.replace_table(current_table, new_table)
                for alias_query in this._with
            ]
            this._selects = [
                select.replace_table(current_table, new_table) for select in this._selects
            ]
            this._columns = [
                column.replace_table(current_table, new_table) for column in this._columns
            ]
            this._values = [
                [value.replace_table(current_table, new_table) for value in value_list]
                for value_list in this._values
            ]

            this._wheres = (
                this._wheres.replace_table(current_table, new_table)
                if this._wheres
                else None
            )
            this._prewheres = (
                this._prewheres.replace_table(current_table, new_table)
                if this._prewheres
                else None
            )
            this._groupbys = [
                groupby.replace_table(current_table, new_table)
                for groupby in this._groupbys
            ]
            this._havings = (
                this._havings.replace_table(current_table, new_table)
                if this._havings
                else None
            )
            this._orderbys = [
                (orderby[0].replace_table(current_table, new_table), orderby[1])
                for orderby in this._orderbys
            ]
            this._joins = [
                join.replace_table(current_table, new_table) for join in this._joins
            ]

            if current_table in this._select_star_tables:
                this._select_star_tables.remove(current_table)
                this._select_star_tables.add(new_table)

            return this

    def with_(self, selectable: Selectable, name: str) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            t = AliasedQuery(name, selectable)
            this._with.append(t)
            return this

    def into(self, table: Union[str, Table]) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            if this._insert_table is not None:
                raise AttributeError("'Query' object has no attribute '%s'" % "into")

            if this._selects:
                this._select_into = True

            this._insert_table = table if isinstance(table, Table) else Table(table)

            return this

    def select(self, *terms: Any) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            for term in terms:
                if isinstance(term, Field):
                    this._select_field(term)
                elif isinstance(term, str):
                    this._select_field_str(term)
                elif isinstance(term, (Function, ArithmeticExpression)):
                    this._select_other(term)
                else:
                    this._select_other(
                        this.wrap_constant(term, wrapper_cls=this._wrapper_cls)
                    )
            return this

    def delete(self) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            if this._delete_from or this._selects or this._update_table:
                raise AttributeError("'Query' object has no attribute '%s'" % "delete")

            this._delete_from = True
            return this

    def update(self, table: Union[str, Table]) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            if this._update_table is not None or this._selects or this._delete_from:
                raise AttributeError("'Query' object has no attribute '%s'" % "update")

            this._update_table = table if isinstance(table, Table) else Table(table)
            return this

    def columns(self, *terms: Any) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            if this._insert_table is None:
                raise AttributeError("'Query' object has no attribute '%s'" % "insert")

            if terms and isinstance(terms[0], (list, tuple)):
                terms = terms[0]

            for term in terms:
                if isinstance(term, str):
                    term = Field(term, table=this._insert_table)
                this._columns.append(term)
            return this

    def insert(self, *terms: Any) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            if this._insert_table is None:
                raise AttributeError("'Query' object has no attribute '%s'" % "insert")

            if not terms:
                return this
            else:
                this._validate_terms_and_append(*terms)
            this._replace = False
            return this

    def replace(self, *terms: Any) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            if this._insert_table is None:
                raise AttributeError("'Query' object has no attribute '%s'" % "insert")

            if not terms:
                return this
            else:
                this._validate_terms_and_append(*terms)
            this._replace = True
            return this

    def force_index(self, term: Union[str, Index], *terms: Union[str, Index]) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            for t in (term, *terms):
                if isinstance(t, Index):
                    this._force_indexes.append(t)
                elif isinstance(t, str):
                    this._force_indexes.append(Index(t))
            return this

    def use_index(self, term: Union[str, Index], *terms: Union[str, Index]) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            for t in (term, *terms):
                if isinstance(t, Index):
                    this._use_indexes.append(t)
                elif isinstance(t, str):
                    this._use_indexes.append(Index(t))
            return this

    def distinct(self) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            this._distinct = True
            return this

    def for_update(self) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            this._for_update = True
            return this

    def ignore(self) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            this._ignore = True
            return this

    def prewhere(self, criterion: Criterion) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            if not this._validate_table(criterion):
                this._foreign_table = True

            if this._prewheres:
                this._prewheres &= criterion
            else:
                this._prewheres = criterion
            return this

    def where(self, criterion: Union[Term, EmptyCriterion]) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            if isinstance(criterion, EmptyCriterion):
                return this

            if not this._validate_table(criterion):
                this._foreign_table = True

            if this._wheres:
                this._wheres &= criterion
            else:
                this._wheres = criterion
            return this

    def having(self, criterion: Term) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            if this._havings:
                this._havings &= criterion
            else:
                this._havings = criterion
            return this

    def groupby(self, *terms: Union[str, int, Term]) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            for term in terms:
                if isinstance(term, str):
                    term = Field(term, table=this._from[0])
                elif isinstance(term, int):
                    term = Field(str(term), table=this._from[0]).wrap_constant(term)

                this._groupbys.append(term)
            return this

    def with_totals(self) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            this._with_totals = True
            return this

    def rollup(self, *terms: Union[list, tuple, set, Term], **kwargs: Any) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            for_mysql = "mysql" == kwargs.get("vendor")

            if this._mysql_rollup:
                raise AttributeError("'Query' object has no attribute '%s'" % "rollup")

            terms = [
                Tuple(*term) if isinstance(term, (list, tuple, set)) else term
                for term in terms
            ]

            if for_mysql:
                # MySQL rolls up all of the dimensions always
                if not terms and not this._groupbys:
                    raise RollupException(
                        "At least one group is required. Call Query.groupby(term) or pass"
                        "as parameter to rollup."
                    )

                this._mysql_rollup = True
                this._groupbys += terms

            elif 0 < len(this._groupbys) and isinstance(this._groupbys[-1], Rollup):
                # If a rollup was added last, then append the new terms to the previous rollup
                this._groupbys[-1].args += terms

            else:
                this._groupbys.append(Rollup(*terms))
            return this

    def orderby(self, *fields: Any, **kwargs: Any) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            for field in fields:
                field = (
                    Field(field, table=this._from[0])
                    if isinstance(field, str)
                    else this.wrap_constant(field)
                )

                this._orderbys.append((field, kwargs.get("order")))
            return this

    def join(
        self, item: Union[Table, "QueryBuilder", AliasedQuery, Selectable], how: JoinType = JoinType.inner
    ) -> "Joiner":
        with copy_if_immutable(self) as this:
            if isinstance(item, Table):
                return Joiner(this, item, how, type_label="table")

            elif isinstance(item, QueryBuilder):
                if item.alias is None:
                    this._tag_subquery(item)
                return Joiner(this, item, how, type_label="subquery")

            elif isinstance(item, AliasedQuery):
                return Joiner(this, item, how, type_label="table")

            elif isinstance(item, Selectable):
                return Joiner(this, item, how, type_label="subquery")

            raise ValueError("Cannot join on type '%s'" % type(item))

    def inner_join(self, item: Union[Table, "QueryBuilder", AliasedQuery]) -> "Joiner":
        return self.join(item, JoinType.inner)

    def left_join(self, item: Union[Table, "QueryBuilder", AliasedQuery]) -> "Joiner":
        return self.join(item, JoinType.left)

    def right_join(self, item: Union[Table, "QueryBuilder", AliasedQuery]) -> "Joiner":
        return self.join(item, JoinType.right)

    def outer_join(self, item: Union[Table, "QueryBuilder", AliasedQuery]) -> "Joiner":
        return self.join(item, JoinType.outer)

    def cross_join(self, item: Union[Table, "QueryBuilder", AliasedQuery]) -> "Joiner":
        return self.join(item, JoinType.cross)

    def limit(self, limit: int) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            this._limit = limit
            return this

    def offset(self, offset: int) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            this._offset = offset
            return this

    def union(self, other: "QueryBuilder") -> _SetOperation:
        with copy_if_immutable(self) as this:
            return _SetOperation(
                this, other, SetOperation.union, wrapper_cls=this._wrapper_cls)

    def union_all(self, other: "QueryBuilder") -> _SetOperation:
        with copy_if_immutable(self) as this:
            return _SetOperation(this, other, SetOperation.union_all, wrapper_cls=this._wrapper_cls)

    def intersect(self, other: "QueryBuilder") -> _SetOperation:
        with copy_if_immutable(self) as this:
            return _SetOperation(this, other, SetOperation.intersect, wrapper_cls=this._wrapper_cls)

    def except_of(self, other: "QueryBuilder") -> _SetOperation:
        with copy_if_immutable(self) as this:
            return _SetOperation(this, other, SetOperation.except_of, wrapper_cls=this._wrapper_cls)

    def minus(self, other: "QueryBuilder") -> _SetOperation:
        with copy_if_immutable(self) as this:
            return _SetOperation(this, other, SetOperation.minus, wrapper_cls=this._wrapper_cls)

    def set(self, field: Union[Field, str], value: Any) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            field = Field(field) if not isinstance(field, Field) else field
            this._updates.append((field, this._wrapper_cls(value)))
            return this

    def __add__(self, other: "QueryBuilder") -> _SetOperation:
        return self.union(other)

    def __mul__(self, other: "QueryBuilder") -> _SetOperation:
        return self.union_all(other)

    def __sub__(self, other: "QueryBuilder") -> _SetOperation:
        return self.minus(other)

    def slice(self, slice: slice) -> "QueryBuilder":
        with copy_if_immutable(self) as this:
            this._offset = slice.start
            this._limit = slice.stop
            return this

    def __getitem__(self, item: Any) -> Union["QueryBuilder", Field]:
        if not isinstance(item, slice):
            return super().__getitem__(item)
        return self.slice(item)

    @staticmethod
    def _list_aliases(field_set: Sequence[Field], quote_char: Optional[str] = None) -> List[str]:
        return [
            field.alias or field.get_sql(quote_char=quote_char) for field in field_set
        ]

    def _select_field_str(self, term: str) -> None:
        if 0 == len(self._from):
            raise QueryException(
                "Cannot select {term}, no FROM table specified.".format(term=term)
            )

        if term == "*":
            self._select_star = True
            self._selects = [Star()]
            return

        self._select_field(Field(term, table=self._from[0]))

    def _select_field(self, term: Field) -> None:
        if self._select_star:
            # Do not add select terms after a star is selected
            return

        if term.table in self._select_star_tables:
            # Do not add select terms for table after a table star is selected
            return

        if isinstance(term, Star):
            self._selects = [
                select
                for select in self._selects
                if not hasattr(select, "table") or term.table != select.table
            ]
            self._select_star_tables.add(term.table)

        self._selects.append(term)

    def _select_other(self, function: Function) -> None:
        self._selects.append(function)

    def fields_(self) -> List[Field]:
        # Don't return anything here. Subqueries have their own fields.
        return []

    def do_join(self, join: "Join") -> None:
        base_tables = self._from + [self._update_table] + self._with
        join.validate(base_tables, self._joins)

        table_in_query = any(
            isinstance(clause, Table) and join.item in base_tables
            for clause in base_tables
        )
        if isinstance(join.item, Table) and join.item.alias is None and table_in_query:
            # On the odd chance that we join the same table as the FROM table and don't set an alias
            # FIXME only works once
            join.item.alias = join.item._table_name + "2"

        self._joins.append(join)

    def is_joined(self, table: Table) -> bool:
        return any(table == join.item for join in self._joins)

    def _validate_table(self, term: Term) -> bool:
        """
        Returns False if the term references a table not already part of the
        FROM clause or JOINS and True otherwise.
        """
        base_tables = self._from + [self._update_table]

        for field in term.fields_():
            table_in_base_tables = field.table in base_tables
            table_in_joins = field.table in [join.item for join in self._joins]
            if all(
                [
                    field.table is not None,
                    not table_in_base_tables,
                    not table_in_joins,
                    field.table != self._update_table,
                ]
            ):
                return False
        return True

    def _tag_subquery(self, subquery: "QueryBuilder") -> None:
        subquery.alias = "sq%d" % self._subquery_count
        self._subquery_count += 1

    def _validate_terms_and_append(self, *terms: Any) -> None:
        """
        Handy function for INSERT and REPLACE statements in order to check if
        terms are introduced and how append them to `self._values`
        """
        if not isinstance(terms[0], (list, tuple, set)):
            terms = [terms]

        for values in terms:
            self._values.append(
                [
                    value if isinstance(value, Term) else self.wrap_constant(value)
                    for value in values
                ]
            )

    def __str__(self) -> str:
        return self.get_sql(dialect=self.dialect)

    def __repr__(self) -> str:
        return self.__str__()

    def __eq__(self, other: "QueryBuilder") -> bool:
        if not isinstance(other, QueryBuilder):
            return False

        if not self.alias == other.alias:
            return False

        return True

    def __ne__(self, other: "QueryBuilder") -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        return hash(self.alias) + sum(hash(clause) for clause in self._from)

    def _set_kwargs_defaults(self, kwargs: dict) -> None:
        kwargs.setdefault("quote_char", self.QUOTE_CHAR)
        kwargs.setdefault("secondary_quote_char", self.SECONDARY_QUOTE_CHAR)
        kwargs.setdefault("alias_quote_char", self.ALIAS_QUOTE_CHAR)
        kwargs.setdefault("as_keyword", self.as_keyword)
        kwargs.setdefault("dialect", self.dialect)

    def get_sql(self, with_alias: bool = False, subquery: bool = False, **kwargs: Any) -> str:
        self._set_kwargs_defaults(kwargs)
        if not (
            self._selects
            or self._insert_table
            or self._delete_from
            or self._update_table
        ):
            return ""
        if self._insert_table and not (self._selects or self._values):
            return ""
        if self._update_table and not self._updates:
            return ""

        has_joins = bool(self._joins)
        has_multiple_from_clauses = 1 < len(self._from)
        has_subquery_from_clause = 0 < len(self._from) and isinstance(self._from[0], QueryBuilder)
        has_reference_to_foreign_table = self._foreign_table
        has_update_from = self._update_table and self._from

        kwargs["with_namespace"] = any(
              [
                  has_joins,
                  has_multiple_from_clauses,
                  has_subquery_from_clause,
                  has_reference_to_foreign_table,
                  has_update_from
              ]
        )

        if self._update_table:
            if self._with:
                querystring = self._with_sql(**kwargs)
            else:
                querystring = ""

            querystring += self._update_sql(**kwargs)

            if self._joins:
                querystring += " " + " ".join(
                    join.get_sql(**kwargs) for join in self._joins
                )

            querystring += self._set_sql(**kwargs)

            if self._from:
                querystring += self._from_sql(**kwargs)

            if self._wheres:
                querystring += self._where_sql(**kwargs)

            if self._limit is not None:
                querystring += self._limit_sql()

            return querystring

        if self._delete_from:
            querystring = self._delete_sql(**kwargs)

        elif not self._select_into and self._insert_table:
            if self._with:
                querystring = self._with_sql(**kwargs)
            else:
                querystring = ""

            if self._replace:
                querystring += self._replace_sql(**kwargs)
            else:
                querystring += self._insert_sql(**kwargs)

            if self._columns:
                querystring += self._columns_sql(**kwargs)

            if self._values:
                querystring += self._values_sql(**kwargs)
                return querystring
            else:
                querystring += " " + self._select_sql(**kwargs)

        else:
            if self._with:
                querystring = self._with_sql(**kwargs)
            else:
                querystring = ""

            querystring += self._select_sql(**kwargs)

            if self._insert_table:
                querystring += self._into_sql(**kwargs)

        if self._from:
            querystring += self._from_sql(**kwargs)

        if self._force_indexes:
            querystring += self._force_index_sql(**kwargs)

        if self._use_indexes:
            querystring += self._use_index_sql(**kwargs)

        if self._joins:
            querystring += " " + " ".join(
                join.get_sql(**kwargs) for join in self._joins
            )

        if self._prewheres:
            querystring += self._prewhere_sql(**kwargs)

        if self._wheres:
            querystring += self._where_sql(**kwargs)

        if self._groupbys:
            querystring += self._group_sql(**kwargs)
            if self._mysql_rollup:
                querystring += self._rollup_sql()

        if self._havings:
            querystring += self._having_sql(**kwargs)

        if self._orderbys:
            querystring += self._orderby_sql(**kwargs)

        querystring = self._apply_pagination(querystring)

        if self._for_update:
            querystring += self._for_update_sql()

        if subquery:
            querystring = "({query})".format(query=querystring)

        if with_alias:
            kwargs['alias_quote_char'] = (self.ALIAS_QUOTE_CHAR
                                          if self.QUERY_ALIAS_QUOTE_CHAR is None
                                          else self.QUERY_ALIAS_QUOTE_CHAR)
            return format_alias_sql(querystring, self.alias, **kwargs)

        return querystring

    def _apply_pagination(self, querystring: str) -> str:
        if self._limit is not None:
            querystring += self._limit_sql()

        if self._offset:
            querystring += self._offset_sql()

        return querystring

    def _with_sql(self, **kwargs: Any) -> str:
        return "WITH " + ",".join(
            clause.name
            + " AS ("
            + clause.get_sql(subquery=False, with_alias=False, **kwargs)
            + ") "
            for clause in self._with
        )

    def _distinct_sql(self, **kwargs: Any) -> str:
        if self._distinct:
            distinct = 'DISTINCT '
        else:
            distinct = ''

        return distinct

    def _for_update_sql(self) -> str:
        if self._for_update:
            for_update = ' FOR UPDATE'
        else:
            for_update = ''

        return for_update

    def _select_sql(self, **kwargs: Any) -> str:
        return "SELECT {distinct}{select}".format(
            distinct=self._distinct_sql(**kwargs),
            select=",".join(
                term.get_sql(with_alias=True, subquery=True, **kwargs)
                for term in self._selects
            ),
        )

    def _insert_sql(self, **kwargs: Any) -> str:
        return "INSERT {ignore}INTO {table}".format(
            table=self._insert_table.get_sql(**kwargs),
            ignore="IGNORE " if self._ignore else "",
        )

    def _replace_sql(self, **kwargs: Any) -> str:
        return "REPLACE INTO {table}".format(
            table=self._insert_table.get_sql(**kwargs),
        )

    @staticmethod
    def _delete_sql(**kwargs: Any) -> str:
        return "DELETE"

    def _update_sql(self, **kwargs: Any) -> str:
        return "UPDATE {table}".format(table=self._update_table.get_sql(**kwargs))

    def _columns_sql(self, with_namespace: bool = False, **kwargs: Any) -> str:
        """
        SQL for Columns clause for INSERT queries
        :param with_namespace:
            Remove from kwargs, never format the column terms with namespaces since only one table can be inserted into
        """
        return " ({columns})".format(
            columns=",".join(
                term.get_sql(with_namespace=False, **kwargs) for term in self._columns
            )
        )

    def _values_sql(self, **kwargs: Any) -> str:
        return " VALUES ({values})".format(
            values="),(".join(
                ",".join(
                    term.get_sql(with_alias=True, subquery=True, **kwargs)
                    for term in row
                )
                for row in self._values
            )
        )

    def _into_sql(self, **kwargs: Any) -> str:
        return " INTO {table}".format(
            table=self._insert_table.get_sql(with_alias=False, **kwargs),
        )

    def _from_sql(self, with_namespace: bool = False, **kwargs: Any) -> str:
        return " FROM {selectable}".format(
            selectable=",".join(
                clause.get_sql(subquery=True, with_alias=True, **kwargs)
                for clause in self._from
            )
        )

    def _force_index_sql(self, **kwargs: Any) -> str:
        return " FORCE INDEX ({indexes})".format(
            indexes=",".join(index.get_sql(**kwargs) for index in self._force_indexes),
        )

    def _use_index_sql(self, **kwargs: Any) -> str:
        return " USE INDEX ({indexes})".format(
            indexes=",".join(index.get_sql(**kwargs) for index in self._use_indexes),
        )

    def _prewhere_sql(self, quote_char: Optional[str] = None, **kwargs: Any) -> str:
        return " PREWHERE {prewhere}".format(
            prewhere=self._prewheres.get_sql(
                quote_char=quote_char, subquery=True, **kwargs
            )
        )

    def _where_sql(self, quote_char: Optional[str] = None, **kwargs: Any) -> str:
        return " WHERE {where}".format(
            where=self._wheres.get_sql(quote_char=quote_char, subquery=True, **kwargs)
        )

    def _group_sql(
        self, quote_char: Optional[str] = None, alias_quote_char: Optional[str] = None, groupby_alias: bool = True, **kwargs: Any
    ) -> str:
        """
        Produces the GROUP BY part of the query.  This is a list of fields. The clauses are stored in the query under
        self._groupbys as a list fields.

        If an groupby field is used in the select clause,
        determined by a matching alias, and the groupby_alias is set True
        then the GROUP BY clause will use the alias,
        otherwise the entire field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self._selects}
        for field in self._groupbys:
            if groupby_alias and field.alias and field.alias in selected_aliases:
                clauses.append(
                    format_quotes(field.alias, alias_quote_char or quote_char)
                )
            else:
                clauses.append(
                    field.get_sql(
                        quote_char=quote_char,
                        alias_quote_char=alias_quote_char,
                        **kwargs
                    )
                )

        sql = " GROUP BY {groupby}".format(groupby=",".join(clauses))

        if self._with_totals:
            return sql + " WITH TOTALS"

        return sql

    def _orderby_sql(
        self, quote_char: Optional[str] = None, alias_quote_char: Optional[str] = None, orderby_alias: bool = True, **kwargs: Any
    ) -> str:
        """
        Produces the ORDER BY part of the query.  This is a list of fields and possibly their directionality, ASC or
        DESC. The clauses are stored in the query under self._orderbys as a list of tuples containing the field and
        directionality (which can be None).

        If an order by field is used in the select clause,
        determined by a matching, and the orderby_alias
        is set True then the ORDER BY clause will use
        the alias, otherwise the field will be rendered as SQL.
        """
        clauses = []
        selected_aliases = {s.alias for s in self._selects}
        for field, directionality in self._orderbys:
            term = (
                format_quotes(field.alias, alias_quote_char or quote_char)
                if orderby_alias and field.alias and field.alias in selected_aliases
                else field.get_sql(
                    quote_char=quote_char, alias_quote_char=alias_quote_char, **kwargs
                )
            )

            clauses.append(
                "{term} {orient}".format(term=term, orient=directionality.value)
                if directionality is not None
                else term
            )

        return " ORDER BY {orderby}".format(orderby=",".join(clauses))

    def _rollup_sql(self) -> str:
        return " WITH ROLLUP"

    def _having_sql(self, quote_char: Optional[str] = None, **kwargs: Any) -> str:
        return " HAVING {having}".format(
            having=self._havings.get_sql(quote_char=quote_char, **kwargs)
        )

    def _offset_sql(self) -> str:
        return " OFFSET {offset}".format(offset=self._offset)

    def _limit_sql(self) -> str:
        return " LIMIT {limit}".format(limit=self._limit)

    def _set_sql(self, **kwargs: Any) -> str:
        return " SET {set}".format(
            set=",".join(
                "{field}={value}".format(
                    field=field.get_sql(**dict(kwargs, with_namespace=False)), value=value.get_sql(**kwargs)
                )
                for field, value in self._updates
            )
        )


class Joiner:
    def __init__(self, query: QueryBuilder, item: Union[Table, "QueryBuilder", AliasedQuery], how: JoinType, type_label: str) -> None:
        self.query = query
        self.item = item
        self.how = how
        self.type_label = type_label

    def on(self, criterion: Optional[Criterion], collate: Optional[str] = None) -> QueryBuilder:
        if criterion is None:
            raise JoinException(
                "Parameter 'criterion' is required for a "
                "{type} JOIN but was not supplied.".format(type=self.type_label)
            )

        self.query.do_join(JoinOn(self.item, self.how, criterion, collate))
        return self.query

    def on_field(self, *fields: Any) -> QueryBuilder:
        if not fields:
            raise JoinException(
                "Parameter 'fields' is required for a "
                "{type} JOIN but was not supplied.".format(type=self.type_label)
            )

        criterion = None
        for field in fields:
            consituent = Field(field, table=self.query._from[0]) == Field(
                field, table=self.item
            )
            criterion = consituent if criterion is None else criterion & consituent

        self.query.do_join(JoinOn(self.item, self.how, criterion))
        return self.query

    def using(self, *fields: Any) -> QueryBuilder:
        if not fields:
            raise JoinException(
                "Parameter 'fields' is required when joining with "
                "a using clause but was not supplied.".format(type=self.type_label)
            )

        self.query.do_join(
            JoinUsing(self.item, self.how, [Field(field) for field in fields])
        )
        return self.query

    def cross(self) -> QueryBuilder:
        """Return cross join"""
        self.query.do_join(Join(self.item, JoinType.cross))

        return self.query


class Join:
    def __init__(self, item: Term, how: JoinType) -> None:
        self.item = item
        self.how = how

    def get_sql(self, **kwargs: Any) -> str:
        sql = "JOIN {table}".format(
              table=self.item.get_sql(subquery=True, with_alias=True, **kwargs),
        )

        if self.how.value:
            return "{type} {join}".format(join=sql, type=self.how.value)
        return sql

    def validate(self, _from: Sequence[Table], _joins: Sequence[Table]) -> None:
        pass

    def replace_table(self, current_table: Optional[Table], new_table: Optional[Table]) -> "Join":
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing
        fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the join with the tables replaced.
        """
        with copy_if_immutable(self) as this:
            this.item = this.item.replace_table(current_table, new_table)
            return this


class JoinOn(Join):
    def __init__(self, item: Term, how: JoinType, criteria: QueryBuilder, collate: Optional[str] = None) -> None:
        super().__init__(item, how)
        self.criterion = criteria
        self.collate = collate

    def get_sql(self, **kwargs: Any) -> str:
        join_sql = super().get_sql(**kwargs)
        return "{join} ON {criterion}{collate}".format(
            join=join_sql,
            criterion=self.criterion.get_sql(subquery=True, **kwargs),
            collate=" COLLATE {}".format(self.collate) if self.collate else "",
        )

    def validate(self, _from: Sequence[Table], _joins: Sequence[Table]) -> None:
        criterion_tables = set([f.table for f in self.criterion.fields_()])
        available_tables = set(_from) | {join.item for join in _joins} | {self.item}
        missing_tables = criterion_tables - available_tables
        if missing_tables:
            raise JoinException(
                "Invalid join criterion. One field is required from the joined item and "
                "another from the selected table or an existing join.  Found [{tables}]".format(
                    tables=", ".join(map(str, missing_tables))
                )
            )

    def replace_table(self, current_table: Optional[Table], new_table: Optional[Table]) -> "JoinOn":
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing
        fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the join with the tables replaced.
        """
        with copy_if_immutable(self) as this:
            this.item = new_table if this.item == current_table else this.item
            this.criterion = this.criterion.replace_table(current_table, new_table)
            return this


class JoinUsing(Join):
    def __init__(self, item: Term, how: JoinType, fields: Sequence[Field]) -> None:
        super().__init__(item, how)
        self.fields = fields

    def get_sql(self, **kwargs: Any) -> str:
        join_sql = super().get_sql(**kwargs)
        return "{join} USING ({fields})".format(
            join=join_sql,
            fields=",".join(field.get_sql(**kwargs) for field in self.fields),
        )

    def validate(self, _from: Sequence[Table], _joins: Sequence[Table]) -> None:
        pass

    def replace_table(self, current_table: Optional[Table], new_table: Optional[Table]) -> "JoinUsing":
        """
        Replaces all occurrences of the specified table with the new table. Useful when reusing
        fields across queries.

        :param current_table:
            The table to be replaced.
        :param new_table:
            The table to replace with.
        :return:
            A copy of the join with the tables replaced.
        """
        with copy_if_immutable(self) as this:
            this.item = new_table if this.item == current_table else this.item
            this.fields = [
                field.replace_table(current_table, new_table) for field in this.fields
            ]
            return this


class CreateQueryBuilder:
    """
    Query builder used to build CREATE queries.
    """

    QUOTE_CHAR = '"'
    SECONDARY_QUOTE_CHAR = "'"
    ALIAS_QUOTE_CHAR = None

    def __init__(self, dialect: Optional[Dialects] = None) -> None:
        self._create_table = None
        self._temporary = False
        self._as_select = None
        self._columns = []
        self._period_fors = []
        self._with_system_versioning = False
        self._primary_key = None
        self._uniques = []
        self.dialect = dialect

    def _set_kwargs_defaults(self, kwargs: dict) -> None:
        kwargs.setdefault("quote_char", self.QUOTE_CHAR)
        kwargs.setdefault("secondary_quote_char", self.SECONDARY_QUOTE_CHAR)
        kwargs.setdefault("dialect", self.dialect)

    def create_table(self, table: Union[Table, str]) -> "CreateQueryBuilder":
        """
        Creates the table.

        :param table:
            An instance of a Table object or a string table name.

        :raises AttributeError:
            If the table is already created.

        :return:
            CreateQueryBuilder.
        """
        with copy_if_immutable(self) as this:
            if this._create_table:
                raise AttributeError("'Query' object already has attribute create_table")

            this._create_table = table if isinstance(table, Table) else Table(table)
            return this

    def temporary(self) -> "CreateQueryBuilder":
        """
        Makes the table temporary.

        :return:
            CreateQueryBuilder.
        """
        with copy_if_immutable(self) as this:
            this._temporary = True
            return this

    def with_system_versioning(self) -> "CreateQueryBuilder":
        """
        Adds system versioning.

        :return:
            CreateQueryBuilder.
        """
        with copy_if_immutable(self) as this:
            this._with_system_versioning = True
            return this

    def columns(self, *columns: Union[str, TypedTuple[str, str], Column]) -> "CreateQueryBuilder":
        """
        Adds the columns.

        :param columns:
            Type:  Union[str, TypedTuple[str, str], Column]

            A list of columns.

        :raises AttributeError:
            If the table is an as_select table.

        :return:
            CreateQueryBuilder.
        """
        with copy_if_immutable(self) as this:
            if this._as_select:
                raise AttributeError("'Query' object already has attribute as_select")

            for column in columns:
                if isinstance(column, str):
                    column = Column(column)
                elif isinstance(column, tuple):
                    column = Column(column_name=column[0], column_type=column[1])
                this._columns.append(column)
            return this

    def period_for(self, name, start_column: Union[str, Column], end_column: Union[str, Column]) -> "CreateQueryBuilder":
        """
        Adds a PERIOD FOR clause.

        :param name:
            The period name.

        :param start_column:
            The column that starts the period.

        :param end_column:
            The column that ends the period.

        :return:
            CreateQueryBuilder.
        """
        with copy_if_immutable(self) as this:
            this._period_fors.append(PeriodFor(name, start_column, end_column))
            return this

    def unique(self, *columns: Union[str, Column]) -> "CreateQueryBuilder":
        """
        Adds a UNIQUE constraint.

        :param columns:
            Type:  Union[str, TypedTuple[str, str], Column]

            A list of columns.

        :return:
            CreateQueryBuilder.
        """
        with copy_if_immutable(self) as this:
            this._uniques.append([
                (column if isinstance(column, Column) else Column(column))
                for column in columns
            ])
            return this

    def primary_key(self, *columns: Union[str, Column]) -> "CreateQueryBuilder":
        """
        Adds a primary key constraint.

        :param columns:
            Type:  Union[str, TypedTuple[str, str], Column]

            A list of columns.

        :raises AttributeError:
            If the primary key is already defined.

        :return:
            CreateQueryBuilder.
        """
        with copy_if_immutable(self) as this:
            if this._primary_key:
                raise AttributeError("'Query' object already has attribute primary_key")
            this._primary_key = [
                (column if isinstance(column, Column) else Column(column))
                for column in columns
            ]
            return this

    def as_select(self, query_builder: QueryBuilder) -> "CreateQueryBuilder":
        """
        Creates the table from a select statement.

        :param query_builder:
            The query.

        :raises AttributeError:
            If columns have been defined for the table.

        :return:
            CreateQueryBuilder.
        """
        with copy_if_immutable(self) as this:
            if this._columns:
                raise AttributeError("'Query' object already has attribute columns")

            if not isinstance(query_builder, QueryBuilder):
                raise TypeError("Expected 'item' to be instance of QueryBuilder")

            this._as_select = query_builder
            return this

    def get_sql(self, **kwargs: Any) -> str:
        """
        Gets the sql statement string.

        :return: The create table statement.
        :rtype: str
        """
        self._set_kwargs_defaults(kwargs)

        if not self._create_table:
            return ""

        if not self._columns and not self._as_select:
            return ""

        create_table = self._create_table_sql(**kwargs)

        if self._as_select:
            return create_table + self._as_select_sql(**kwargs)

        body = self._body_sql(**kwargs)
        table_options = self._table_options_sql(**kwargs)

        return "{create_table} ({body}){table_options}".format(
            create_table=create_table,
            body=body,
            table_options=table_options
        )

    def _create_table_sql(self, **kwargs: Any) -> str:
        return "CREATE {temporary}TABLE {table}".format(
            temporary="TEMPORARY " if self._temporary else "",
            table=self._create_table.get_sql(**kwargs),
        )

    def _table_options_sql(self, **kwargs) -> str:
        table_options = ""

        if self._with_system_versioning:
            table_options += ' WITH SYSTEM VERSIONING'

        return table_options

    def _column_clauses(self, **kwargs) -> List[str]:
        return [
            column.get_sql(**kwargs)
            for column in self._columns
        ]

    def _period_for_clauses(self, **kwargs) -> List[str]:
        return [
            period_for.get_sql(**kwargs)
            for period_for in self._period_fors
        ]

    def _unique_key_clauses(self, **kwargs) -> List[str]:
        return [
            "UNIQUE ({unique})".format(
                unique=",".join(
                    column.get_name_sql(**kwargs)
                    for column in unique)
            )
            for unique in self._uniques
        ]

    def _primary_key_clause(self, **kwargs) -> str:
        return "PRIMARY KEY ({columns})".format(
            columns=",".join(
                column.get_name_sql(**kwargs)
                for column in self._primary_key
            )
        )

    def _body_sql(self, **kwargs) -> str:
        clauses = self._column_clauses(**kwargs)
        clauses += self._period_for_clauses(**kwargs)
        clauses += self._unique_key_clauses(**kwargs)

        # Primary keys
        if self._primary_key:
            clauses.append(self._primary_key_clause(**kwargs))

        return ",".join(clauses)

    def _as_select_sql(self, **kwargs: Any) -> str:
        return " AS ({query})".format(query=self._as_select.get_sql(**kwargs), )

    def __str__(self) -> str:
        return self.get_sql()

    def __repr__(self) -> str:
        return self.__str__()
