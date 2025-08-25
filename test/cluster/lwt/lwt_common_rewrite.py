"""
Common functionality for LWT (Lightweight Transaction)
This module provides shared components for testing LWT behavior during various tablet operations
like migrations, splits, merges, etc.
"""

import asyncio
import logging
import random
import re
import time
from dataclasses import dataclass
from functools import cached_property

from cassandra import ConsistencyLevel
from cassandra import WriteTimeout, ReadTimeout, OperationTimedOut
from cassandra.query import SimpleStatement, PreparedStatement

from test.pylib.manager_client import ManagerClient
from test.pylib.tablets import get_tablet_count

logger = logging.getLogger(__name__)

# Test constants (arbitrary values)
DEFAULT_WORKERS = 20
DEFAULT_BACKOFF_BASE = 0.02
DEFAULT_NUM_KEYS = 50
UNCERTAINTY_RE = re.compile(r"write timeout due to uncertainty", re.IGNORECASE)


@dataclass
class LWTNonApplyError(RuntimeError):
    pk: int
    worker_id: int
    prev_val: object
    new_val: object
    operation_id: str

    def __str__(self) -> str:
        return (
            f"Unexpected CAS non-apply without timeout: "
            f"pk={self.pk} worker={self.worker_id} prev={self.prev_val}, new={self.new_val}"
        )


class Worker:
    def __init__(
        self,
        worker_id: int,
        cql,
        pks: list[int],
        select_statement: PreparedStatement,
        update_statement: PreparedStatement,
        other_columns: list[int],
    ):
        super().__init__()
        self.success_counts: dict[int, int] = {pk: 0 for pk in pks}
        self.worker_id = worker_id
        self.rng = random.Random()
        self.select_statement = select_statement
        self.update_statement = update_statement
        self.others_columns = other_columns
        self.pks = pks
        self.cql = cql
        self.stop_event = asyncio.Event()

    async def stop(self):
        self.stop_event.set()

    async def __call__(self):
        operation_id = 0
        while not self.stop_event.is_set():
            operation_id += 1
            pk = self.rng.choice(self.pks)

            # Read current values
            verify_query = self.select_statement.bind([pk])
            verify_query.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            rows = await self.cql.run_async(verify_query)

            row = rows[0]
            prev_val = getattr(row, f"s{self.worker_id}")
            expected = self.success_counts[pk]
            new_val = expected + 1

            # Verify consistency before update
            assert prev_val == expected, (
                f"tracker mismatch: pk={pk} s{self.worker_id} row={prev_val} tracker={expected}"
            )

            # Prepare conditional update
            update = self.update_statement.bind(
                [
                    new_val,
                    pk,
                    *(getattr(row, f"s{col_idx}") for col_idx in self.others_columns),
                    prev_val,
                ]
            )
            update.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            update.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
            applied = False
            try:
                res = await self.cql.run_async(update)
                applied = bool(res and res[0].applied)
            except (WriteTimeout, OperationTimedOut, ReadTimeout) as e:
                if not is_uncertainty_timeout(e):
                    raise
                verify_stmt = self.select_statement.bind([pk])
                verify_stmt.consistency_level = ConsistencyLevel.LOCAL_SERIAL
                # TODO: not sure how to simplify this retry logic
                while True:
                    try:
                        rows = await self.cql.run_async(verify_stmt)
                        vrow = rows[0]
                        current_val = getattr(vrow, f"s{self.worker_id}")
                        assert current_val == new_val or current_val == prev_val
                        applied = current_val == new_val
                        break
                    except (WriteTimeout, OperationTimedOut, ReadTimeout):
                        await asyncio.sleep(0.05)
                        continue

            if applied:
                self.success_counts[pk] = new_val
            else:
                raise LWTNonApplyError(
                    pk=pk,
                    worker_id=self.worker_id,
                    prev_val=prev_val,
                    new_val=new_val,
                    operation_id=operation_id,
                )

            if self.stop_event.is_set():
                break


class BaseLWTTester:
    """
    Base class for coordinating multi-column LWT testing using dedicated components.
    """

    def __init__(
        self,
        manager: ManagerClient,
        ks: str,
        tbl: str,
        num_workers: int = DEFAULT_WORKERS,
        num_keys: int = DEFAULT_NUM_KEYS,
    ):
        self.ks = ks
        self.tbl = tbl
        self.cql = manager.get_cql()
        self.num_workers = num_workers
        self.pks = list(range(1, num_keys + 1))
        self.workers = self.create_workers()
        self.select_cols = ", ".join(f"s{i}" for i in range(self.num_workers))

    @cached_property
    def select_statement(self):
        return self.cql.prepare(f"SELECT {self.select_cols} FROM {self.ks}.{self.tbl} WHERE pk = ?")

    def create_workers(self):
        workers = []
        for i in range(self.num_workers):
            other_columns = [j for j in range(self.num_workers) if j != i]
            cond = " AND ".join([*(f"s{j} >= ?" for j in other_columns), f"s{i} = ?"])
            query = f"UPDATE {self.ks}.{self.tbl} SET s{i} = ? WHERE pk = ? IF {cond} "
            worker = Worker(
                worker_id=i,
                cql=self.cql,
                pks=self.pks,
                select_statement=self.select_statement,
                update_statement=self.cql.prepare(query),
                other_columns=other_columns,
            )
            workers.append(worker)

    async def create_schema(self):
        """Create table with multiple counter columns"""
        cols_def = ", ".join(f"s{i} int" for i in range(self.num_workers))
        await self.cql.run_async(f"CREATE TABLE {self.ks}.{self.tbl} (pk int PRIMARY KEY, {cols_def})")
        logger.info("Created table %s.%s with %d columns", self.ks, self.tbl, self.num_workers)

    async def initialize_rows(self):
        """Initialize the test rows with all columns set to 0"""
        zeros = ", ".join("0" for _ in range(self.num_workers))
        ps = self.cql.prepare(f"INSERT INTO {self.ks}.{self.tbl} (pk, {self.select_cols}) VALUES (?, {zeros})")
        for pk in self.pks:
            await self.cql.run_async(ps.bind([pk]))

    async def start_workers(self):
        """Start workload workers"""
        for worker in self.workers:
            asyncio.create_task(worker())

    async def stop_workers(self):
        """Stop workload workers"""
        for worker in self.workers:
            worker.stop()

    async def verify_consistency(self):
        """Ensure every (pk, column) reflects the number of successful CAS writes."""
        mismatches = []
        # limit_clause = " LIMIT 1" if getattr(self.schema, "uses_static", False) else ""
        for pk in self.pks:
            stmt = SimpleStatement(
                f"SELECT {self.select_cols} FROM {self.ks}.{self.tbl} WHERE pk = %s",
                consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            )
            row = (await self.cql.run_async(stmt, [pk]))[0]
            for i, worker in enumerate(self.workers):
                actual = getattr(row, f"s{i}")
                expected = worker.success_counts[pk]
                if actual != expected:
                    mismatches.append(f"pk={pk} s{i}={actual}, expected {expected}")

        assert not mismatches, "Consistency violations: " + "; ".join(mismatches) if mismatches else ""
        total_ops = sum(sum(v) for worker in self.workers for v in worker.success_counts.values())
        logger.info("Consistency verified – %d total successful CAS operations", total_ops)


# Utility functions for tablet operations
async def get_token_for_pk(cql, ks: str, tbl: str, pk: int) -> int:
    """Get the token for a given primary key"""
    stmt = SimpleStatement(
        f"SELECT token(pk) AS tk FROM {ks}.{tbl} WHERE pk = %s",
        consistency_level=ConsistencyLevel.ONE,
    )
    row = (await cql.run_async(stmt, [pk]))[0]
    return row.tk


async def get_host_map(manager: ManagerClient, servers):
    """Create a mapping from host IDs to server info"""
    ids = await asyncio.gather(*[manager.get_host_id(s.server_id) for s in servers])
    return {hid: srv for hid, srv in zip(ids, servers)}


async def pick_non_replica_server(manager: ManagerClient, servers, replica_host_ids):
    """Find a server that is not a replica for the given tablet"""
    for s in servers:
        hid = await manager.get_host_id(s.server_id)
        if hid not in replica_host_ids:
            return s
    return None


async def wait_for_tablet_count(
    manager: ManagerClient,
    server,
    ks: str,
    tbl: str,
    predicate,
    target: int,
    timeout_s: int = 180,
    poll_s: float = 1.0,
):
    """
    Wait for tablet count to match predicate.
    predicate: callable like lambda c: c >= target (for split) or lambda c: c <= target (for merge)
    """
    deadline = time.time() + timeout_s
    last = None
    while time.time() < deadline:
        count = await get_tablet_count(manager, server, ks, tbl)
        last = count
        if predicate(count):
            return count
        await asyncio.sleep(poll_s)
    raise TimeoutError(f"tablet count wait timed out (last={last}, target={target})")


def is_uncertainty_timeout(exc: Exception) -> bool:
    return isinstance(exc, (WriteTimeout, ReadTimeout)) and UNCERTAINTY_RE.search(str(exc)) is not None
