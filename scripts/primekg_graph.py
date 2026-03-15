#!/usr/bin/env python3
"""Build and query a local PrimeKG property graph in SQLite.

This script ingests PrimeKG CSV files (`nodes.csv`, `edges.csv`) into a SQLite
database so the full graph can be queried without loading all edges into memory.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Iterable

from dotenv import load_dotenv
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError, ServiceUnavailable

DEFAULT_DB_PATH = Path("data/primekg.sqlite")
DEFAULT_NODES_CSV = Path("data/nodes.csv")
DEFAULT_EDGES_CSV = Path("data/edges.csv")


def _batched(rows: Iterable[tuple], batch_size: int) -> Iterable[list[tuple]]:
    batch: list[tuple] = []
    for row in rows:
        batch.append(row)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    return conn


def create_schema(conn: sqlite3.Connection, reset: bool = False) -> None:
    if reset:
        conn.executescript(
            """
            DROP TABLE IF EXISTS edges;
            DROP TABLE IF EXISTS nodes;
            """
        )

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS nodes (
            node_index INTEGER PRIMARY KEY,
            node_id TEXT NOT NULL,
            node_type TEXT NOT NULL,
            node_name TEXT NOT NULL,
            node_source TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS edges (
            edge_id INTEGER PRIMARY KEY AUTOINCREMENT,
            relation TEXT NOT NULL,
            display_relation TEXT NOT NULL,
            x_index INTEGER NOT NULL,
            y_index INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_nodes_type ON nodes(node_type);
        CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(node_name);
        CREATE INDEX IF NOT EXISTS idx_edges_x ON edges(x_index);
        CREATE INDEX IF NOT EXISTS idx_edges_y ON edges(y_index);
        CREATE INDEX IF NOT EXISTS idx_edges_relation ON edges(relation);
        """
    )
    conn.commit()


def ingest_nodes(conn: sqlite3.Connection, nodes_csv: Path, batch_size: int) -> int:
    with nodes_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = (
            (
                int(r["node_index"]),
                r["node_id"],
                r["node_type"],
                r["node_name"],
                r["node_source"],
            )
            for r in reader
        )
        inserted = 0
        for batch in _batched(rows, batch_size=batch_size):
            conn.executemany(
                """
                INSERT OR REPLACE INTO nodes
                (node_index, node_id, node_type, node_name, node_source)
                VALUES (?, ?, ?, ?, ?)
                """,
                batch,
            )
            inserted += len(batch)
        conn.commit()
    return inserted


def ingest_edges(conn: sqlite3.Connection, edges_csv: Path, batch_size: int) -> int:
    with edges_csv.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = (
            (
                r["relation"],
                r["display_relation"],
                int(r["x_index"]),
                int(r["y_index"]),
            )
            for r in reader
        )
        inserted = 0
        for batch in _batched(rows, batch_size=batch_size):
            conn.executemany(
                """
                INSERT INTO edges (relation, display_relation, x_index, y_index)
                VALUES (?, ?, ?, ?)
                """,
                batch,
            )
            inserted += len(batch)
        conn.commit()
    return inserted


def cmd_build(args: argparse.Namespace) -> None:
    conn = _connect(args.db)
    try:
        start = time.time()
        create_schema(conn, reset=args.reset)

        node_count = ingest_nodes(conn, args.nodes, batch_size=args.batch_size)
        edge_count = ingest_edges(conn, args.edges, batch_size=args.batch_size)

        elapsed = time.time() - start
        print(
            f"Build complete. Nodes: {node_count:,} | Edges: {edge_count:,} | "
            f"Time: {elapsed:.1f}s | DB: {args.db}"
        )
    finally:
        conn.close()


def cmd_stats(args: argparse.Namespace) -> None:
    conn = _connect(args.db)
    try:
        node_count = conn.execute("SELECT COUNT(*) FROM nodes").fetchone()[0]
        edge_count = conn.execute("SELECT COUNT(*) FROM edges").fetchone()[0]
        print(f"Nodes: {node_count:,}")
        print(f"Edges: {edge_count:,}")

        print("\nTop node types:")
        for node_type, count in conn.execute(
            """
            SELECT node_type, COUNT(*) AS c
            FROM nodes
            GROUP BY node_type
            ORDER BY c DESC
            LIMIT 10
            """
        ):
            print(f"  {node_type:<20} {count:,}")

        print("\nTop relations:")
        for rel, count in conn.execute(
            """
            SELECT relation, COUNT(*) AS c
            FROM edges
            GROUP BY relation
            ORDER BY c DESC
            LIMIT 15
            """
        ):
            print(f"  {rel:<30} {count:,}")
    finally:
        conn.close()


def cmd_neighbors(args: argparse.Namespace) -> None:
    conn = _connect(args.db)
    try:
        node = conn.execute(
            """
            SELECT node_index, node_name, node_type, node_source
            FROM nodes
            WHERE node_index = ?
            """,
            (args.node_index,),
        ).fetchone()

        if not node:
            raise SystemExit(f"Node index {args.node_index} not found.")

        print(
            f"Node {node[0]} | name='{node[1]}' | type={node[2]} | source={node[3]}"
        )

        print("\nOutgoing edges:")
        out_rows = conn.execute(
            """
            SELECT e.relation, n.node_index, n.node_name, n.node_type
            FROM edges e
            JOIN nodes n ON n.node_index = e.y_index
            WHERE e.x_index = ?
            ORDER BY e.relation, n.node_index
            LIMIT ?
            """,
            (args.node_index, args.limit),
        ).fetchall()
        if not out_rows:
            print("  (none)")
        for rel, idx, name, node_type in out_rows:
            print(f"  -[{rel}]-> {idx} | {name} ({node_type})")

        print("\nIncoming edges:")
        in_rows = conn.execute(
            """
            SELECT e.relation, n.node_index, n.node_name, n.node_type
            FROM edges e
            JOIN nodes n ON n.node_index = e.x_index
            WHERE e.y_index = ?
            ORDER BY e.relation, n.node_index
            LIMIT ?
            """,
            (args.node_index, args.limit),
        ).fetchall()
        if not in_rows:
            print("  (none)")
        for rel, idx, name, node_type in in_rows:
            print(f"  <-[{rel}]- {idx} | {name} ({node_type})")
    finally:
        conn.close()


def _neo4j_relationship_type(relation: str) -> str:
    rel = re.sub(r"[^A-Za-z0-9_]", "_", relation).upper()
    if not rel:
        return "RELATED_TO"
    if rel[0].isdigit():
        return f"R_{rel}"
    return rel


def _get_neo4j_credentials() -> tuple[str, str, str, str]:
    load_dotenv()

    uri = os.getenv("NEO4J_URI")
    username = os.getenv("NEO4J_USERNAME")
    password = os.getenv("NEO4J_PASSWORD")
    database = os.getenv("NEO4J_DATABASE", "neo4j")

    missing = [
        key
        for key, value in (
            ("NEO4J_URI", uri),
            ("NEO4J_USERNAME", username),
            ("NEO4J_PASSWORD", password),
        )
        if not value
    ]
    if missing:
        raise SystemExit(
            "Missing Neo4j credentials in .env: " + ", ".join(missing)
        )

    return uri, username, password, database


def _iter_sqlite_batches(cursor: sqlite3.Cursor, batch_size: int) -> Iterable[list[tuple]]:
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows


def _connect_neo4j_with_fallback(uri: str, username: str, password: str):
    driver = GraphDatabase.driver(uri, auth=(username, password))
    try:
        driver.verify_connectivity()
        return driver, uri
    except ServiceUnavailable as first_error:
        driver.close()

        if uri.startswith("neo4j+s://"):
            fallback_uri = "bolt+s://" + uri.split("://", maxsplit=1)[1]
            fallback_driver = GraphDatabase.driver(
                fallback_uri,
                auth=(username, password),
            )
            try:
                fallback_driver.verify_connectivity()
                return fallback_driver, fallback_uri
            except ServiceUnavailable as second_error:
                fallback_driver.close()
                raise SystemExit(
                    "Neo4j connectivity failed for both routing and direct modes. "
                    f"routing error: {first_error}; direct error: {second_error}"
                ) from second_error

        raise SystemExit(f"Neo4j connectivity failed: {first_error}") from first_error


def _is_retryable_entity_not_found(error: Exception) -> bool:
    text = str(error)
    return (
        "Neo.ClientError.Statement.EntityNotFound" in text
        or "Unable to load NODE" in text
    )


def _write_edge_rows_with_fallback(
    neo4j_driver,
    database: str,
    rel_type: str,
    rows: list[dict],
) -> int:
    if not rows:
        return 0

    query = f"""
    UNWIND $rows AS row
    MATCH (x:PrimeNode {{node_index: row.x_index}})
    MATCH (y:PrimeNode {{node_index: row.y_index}})
    CREATE (x)-[r:{rel_type}]->(y)
    SET r.relation = row.relation,
        r.display_relation = row.display_relation
    """

    try:
        neo4j_driver.execute_query(
            query,
            rows=rows,
            database_=database,
        )
        return len(rows)
    except ClientError as error:
        if not _is_retryable_entity_not_found(error):
            raise

        if len(rows) == 1:
            row = rows[0]
            print(
                "Skipped one problematic edge row after Neo4j internal "
                f"EntityNotFound: x_index={row['x_index']} y_index={row['y_index']} "
                f"relation={row['relation']}"
            )
            return 0

        mid = len(rows) // 2
        left_written = _write_edge_rows_with_fallback(
            neo4j_driver,
            database,
            rel_type,
            rows[:mid],
        )
        right_written = _write_edge_rows_with_fallback(
            neo4j_driver,
            database,
            rel_type,
            rows[mid:],
        )
        return left_written + right_written


def cmd_neo4j_sync(args: argparse.Namespace) -> None:
    uri, username, password, database = _get_neo4j_credentials()

    sqlite_conn = _connect(args.db)
    neo4j_driver, connected_uri = _connect_neo4j_with_fallback(
        uri,
        username,
        password,
    )

    try:
        if args.reset_neo4j:
            neo4j_driver.execute_query(
                "MATCH (n:PrimeNode) DETACH DELETE n",
                database_=database,
            )

        neo4j_driver.execute_query(
            """
            CREATE CONSTRAINT prime_node_index_unique IF NOT EXISTS
            FOR (n:PrimeNode) REQUIRE n.node_index IS UNIQUE
            """,
            database_=database,
        )

        node_sql = (
            "SELECT node_index, node_id, node_type, node_name, node_source "
            "FROM nodes ORDER BY node_index"
        )
        if args.node_limit is not None:
            node_sql += " LIMIT ?"
            node_cursor = sqlite_conn.execute(node_sql, (args.node_limit,))
        else:
            node_cursor = sqlite_conn.execute(node_sql)

        node_count = 0
        start = time.time()
        for batch in _iter_sqlite_batches(node_cursor, args.batch_size):
            payload = [
                {
                    "node_index": row[0],
                    "node_id": row[1],
                    "node_type": row[2],
                    "node_name": row[3],
                    "node_source": row[4],
                }
                for row in batch
            ]

            neo4j_driver.execute_query(
                """
                UNWIND $rows AS row
                MERGE (n:PrimeNode {node_index: row.node_index})
                SET n.node_id = row.node_id,
                    n.node_type = row.node_type,
                    n.node_name = row.node_name,
                    n.node_source = row.node_source
                """,
                rows=payload,
                database_=database,
            )
            node_count += len(payload)

        edge_sql = (
            "SELECT relation, display_relation, x_index, y_index "
            "FROM edges ORDER BY edge_id"
        )
        if args.edge_limit is not None:
            edge_sql += " LIMIT ?"
            edge_cursor = sqlite_conn.execute(edge_sql, (args.edge_limit,))
        else:
            edge_cursor = sqlite_conn.execute(edge_sql)

        edge_count = 0
        for batch in _iter_sqlite_batches(edge_cursor, args.batch_size):
            grouped: dict[str, list[dict]] = defaultdict(list)
            for relation, display_relation, x_index, y_index in batch:
                grouped[_neo4j_relationship_type(relation)].append(
                    {
                        "relation": relation,
                        "display_relation": display_relation,
                        "x_index": x_index,
                        "y_index": y_index,
                    }
                )

            for rel_type, rel_rows in grouped.items():
                edge_count += _write_edge_rows_with_fallback(
                    neo4j_driver,
                    database,
                    rel_type,
                    rel_rows,
                )

        elapsed = time.time() - start
        print(
            "Neo4j sync complete. "
            f"Nodes: {node_count:,} | Edges: {edge_count:,} | "
            f"Database: {database} | URI: {connected_uri} | Time: {elapsed:.1f}s"
        )
    finally:
        sqlite_conn.close()
        neo4j_driver.close()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="PrimeKG SQLite graph builder and query CLI"
    )
    parser.set_defaults(func=None)

    shared = argparse.ArgumentParser(add_help=False)
    shared.add_argument(
        "--db",
        type=Path,
        default=DEFAULT_DB_PATH,
        help=f"SQLite DB path (default: {DEFAULT_DB_PATH})",
    )

    p_build = parser.add_subparsers(dest="command", required=True)

    build = p_build.add_parser("build", parents=[shared], help="Build graph DB")
    build.add_argument(
        "--nodes",
        type=Path,
        default=DEFAULT_NODES_CSV,
        help=f"nodes.csv path (default: {DEFAULT_NODES_CSV})",
    )
    build.add_argument(
        "--edges",
        type=Path,
        default=DEFAULT_EDGES_CSV,
        help=f"edges.csv path (default: {DEFAULT_EDGES_CSV})",
    )
    build.add_argument(
        "--batch-size",
        type=int,
        default=20000,
        help="Batch size for inserts (default: 20000)",
    )
    build.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate tables before loading",
    )
    build.set_defaults(func=cmd_build)

    stats = p_build.add_parser("stats", parents=[shared], help="Show graph stats")
    stats.set_defaults(func=cmd_stats)

    neighbors = p_build.add_parser(
        "neighbors", parents=[shared], help="Show node neighbors"
    )
    neighbors.add_argument("node_index", type=int, help="PrimeKG node_index")
    neighbors.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Rows shown for incoming and outgoing edges (default: 20)",
    )
    neighbors.set_defaults(func=cmd_neighbors)

    neo4j_sync = p_build.add_parser(
        "neo4j-sync",
        parents=[shared],
        help="Sync SQLite graph into Neo4j using .env credentials",
    )
    neo4j_sync.add_argument(
        "--batch-size",
        type=int,
        default=2000,
        help="Batch size for Neo4j writes (default: 2000)",
    )
    neo4j_sync.add_argument(
        "--reset-neo4j",
        action="store_true",
        help="Delete existing :PrimeNode subgraph before syncing",
    )
    neo4j_sync.add_argument(
        "--node-limit",
        type=int,
        default=None,
        help="Optional limit for nodes to sync (for testing)",
    )
    neo4j_sync.add_argument(
        "--edge-limit",
        type=int,
        default=None,
        help="Optional limit for edges to sync (for testing)",
    )
    neo4j_sync.set_defaults(func=cmd_neo4j_sync)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
