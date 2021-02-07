"""Node"""

from __future__ import annotations

from typing import Generic, Iterator, List, Type, TypeVar

NodeT = TypeVar("NodeT", bound="Node", covariant=True)


class Node(Generic[NodeT]):

    def nodes_(self) -> Iterator[Node]:
        yield self

    def find_(self, type: Type[NodeT]) -> List[NodeT]:
        return [node for node in self.nodes_() if isinstance(node, type)]
