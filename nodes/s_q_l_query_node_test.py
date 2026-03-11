from gen.messages_pb2 import QueryRequest, QueryResult
from nodes.s_q_l_query_node import s_q_l_query_node


class _NoOpLogger:
    """Minimal AxiomLogger implementation for unit tests."""
    def debug(self, msg: str, **attrs) -> None: pass
    def info(self, msg: str, **attrs) -> None: pass
    def warn(self, msg: str, **attrs) -> None: pass
    def error(self, msg: str, **attrs) -> None: pass


class _NoOpSecrets:
    def get(self, name: str):
        return "", False


def test_s_q_l_query_node_missing_secret():
    """Without a secret, the node should return empty columns and rows."""
    log = _NoOpLogger()
    secrets = _NoOpSecrets()
    req = QueryRequest(query_template="SELECT * FROM users", params=[])
    result = s_q_l_query_node(log, secrets, req)
    assert isinstance(result, QueryResult)
    assert len(result.columns) == 0
    assert len(result.rows) == 0


def test_s_q_l_query_node_rejects_non_select():
    """Non-SELECT statements should be rejected even with a valid (fake) secret."""
    log = _NoOpLogger()

    class _FakeSecrets:
        def get(self, name: str):
            if name == "DATABASE_URL":
                return "postgresql://user:pass@localhost/db", True
            return "", False

    req = QueryRequest(query_template="DROP TABLE users", params=[])
    result = s_q_l_query_node(log, _FakeSecrets(), req)
    assert isinstance(result, QueryResult)
    assert len(result.columns) == 0
