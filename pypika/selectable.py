from __future__ import annotations

from .node import Node
from .terms import Field, Star
from .utils import copy_if_immutable, ignore_copy


class Selectable(Node):

    def __init__(self, alias: str) -> None:
        self.alias = alias

    def as_(self, alias: str) -> Selectable:
        with copy_if_immutable(self) as this:
            this.alias = alias
            return this

    def field(self, name: str) -> Field:
        return Field(name, table=self)

    @property
    def star(self) -> Star:
        return Star(self)

    @ignore_copy
    def __getattr__(self, name: str) -> Field:
        return self.field(name)

    @ignore_copy
    def __getitem__(self, name: str) -> Field:
        return self.field(name)

    def get_table_name(self) -> str:
        return self.alias
