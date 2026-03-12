from gen.messages_pb2 import QueryRequest, QueryResult, Row
from gen.axiom_logger import AxiomLogger, AxiomSecrets

# Module-level connection pool cache keyed by DATABASE_URL. The first invocation
# for a given URL pays the connection cost; subsequent invocations reuse the pool.
_pool_cache: dict = {}
_pool_lock = None


def _get_pool(database_url: str):
    import threading
    global _pool_lock
    if _pool_lock is None:
        _pool_lock = threading.Lock()
    with _pool_lock:
        if database_url not in _pool_cache:
            import psycopg2.pool
            _pool_cache[database_url] = psycopg2.pool.ThreadedConnectionPool(
                minconn=1, maxconn=10, dsn=database_url
            )
    return _pool_cache[database_url]


def sql_query_node(log: AxiomLogger, secrets: AxiomSecrets, input: QueryRequest) -> QueryResult:
    """Executes a parameterised SELECT query against a Postgres database and returns the result rows.

    Reads DATABASE_URL from secrets. Only SELECT statements are permitted — any
    query that does not begin with SELECT (case-insensitive, after trimming) is
    rejected. Connection pools are cached per unique DATABASE_URL so repeated
    invocations reuse existing connections. Users with high-concurrency workloads
    should place a connection pooler (PgBouncer, RDS Proxy) in front of their
    database rather than relying solely on this node-level pool.
    """
    database_url, ok = secrets.get("DATABASE_URL")
    if not ok:
        log.error("sql_query_node: DATABASE_URL secret not found")
        return QueryResult(columns=[], rows=[])

    query = input.query_template.strip()
    if not query.upper().startswith("SELECT"):
        log.error("sql_query_node: only SELECT statements are permitted", query_prefix=query[:20])
        return QueryResult(columns=[], rows=[])

    log.info("sql_query_node: executing query", params=len(input.params))

    pool = _get_pool(database_url)
    conn = pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(query, list(input.params) if input.params else None)
            columns = [desc[0] for desc in cur.description] if cur.description else []
            raw_rows = cur.fetchall()
    finally:
        pool.putconn(conn)

    rows = [Row(values=[str(v) if v is not None else "" for v in row]) for row in raw_rows]
    log.info("sql_query_node: done", columns=len(columns), rows=len(rows))
    return QueryResult(columns=columns, rows=rows)
