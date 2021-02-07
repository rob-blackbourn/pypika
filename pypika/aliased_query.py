from __future__ import annotations

from typing import Any, Optional

from .selectable import Selectable


class AliasedQuery(Selectable):
    def __init__(self, name: str, query: Optional[Selectable] = None) -> None:
        super().__init__(alias=name)
        self.name = name
        self.query = query

    def get_sql(self, **kwargs: Any) -> str:
        if self.query is None:
            return self.name
        return self.query.get_sql(**kwargs)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, AliasedQuery) and self.name == other.name

    def __hash__(self) -> int:
        return hash(str(self.name))
