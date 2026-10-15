from abc import ABC, abstractmethod
from falkordb import FalkorDB
import redis


class GraphDB(ABC):
    @abstractmethod
    def query(self, graph: str, cypher: str, params: dict | None = None):
        pass

    @abstractmethod
    def read(self, graph: str, cypher: str, params: dict | None = None):
        pass

    @abstractmethod
    def ensure_indexes(self, graph: str, statements: list[str]) -> None:
        pass

    @abstractmethod
    def delete_graph(self, graph: str) -> None:
        pass

    @abstractmethod
    def set_state(self, key: str, field: str, value: str) -> None:
        pass

    @abstractmethod
    def get_state(self, key: str) -> dict[str, str]:
        pass


class FalkorGraphDB(GraphDB):
    def __init__(self, host: str = "localhost", port: int = 6380):
        self.db = FalkorDB(host=host, port=port)
        self.client = self.db.connection

    def query(self, graph: str, cypher: str, params: dict | None = None):
        return self.db.select_graph(graph).query(cypher, params)

    def read(self, graph: str, cypher: str, params: dict | None = None):
        return self.db.select_graph(graph).ro_query(cypher, params, timeout=5000)

    def ensure_indexes(self, graph: str, statements: list[str]) -> None:
        for statement in statements:
            try:
                self.query(graph, statement)
            except redis.ResponseError as err:
                if "already indexed" not in str(err).lower():
                    raise

    def delete_graph(self, graph: str) -> None:
        try:
            self.db.select_graph(graph).delete()
        except redis.ResponseError as err:
            if (
                "empty key" not in str(err).lower()
                and "invalid" not in str(err).lower()
            ):
                raise

    def set_state(self, key: str, field: str, value: str) -> None:
        self.client.hset(key, field, value)

    def get_state(self, key: str) -> dict[str, str]:
        return dict(self.client.hgetall(key))
