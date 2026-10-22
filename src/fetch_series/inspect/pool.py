"""Reading and classifying off the event loop, in worker processes.

A content survey is one asyncio process streaming eight files at once. Parsing
100,000 reads and matching every barcode window against every list is CPU work,
and done on the loop it stalled the downloads beside it until they failed with
read errors: the 500-run survey held one core at 100% and finished a run every
four minutes. The work is pure -- bytes in, counts out -- so it moves to worker
processes, with the barcode lists and references sent once per worker rather
than with every call.
"""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor
from typing import Any

from fetch_series.inspect.classify import classify
from fetch_series.inspect.fit import fit
from fetch_series.inspect.lists import BarcodeList
from fetch_series.inspect.model import FileSample
from fetch_series.inspect.qc import library, qc
from fetch_series.inspect.references import KmerIndex
from fetch_series.inspect.tags import available as tag_sets

_WORKER: dict[str, Any] = {}


def _start(lists: dict[str, BarcodeList], references: dict[str, KmerIndex]) -> None:
    _WORKER["lists"], _WORKER["references"] = lists, references


Judged = tuple[dict[str, Any], dict[str, Any], dict[str, Any]]


def judge(
    samples: list[FileSample],
    lists: dict[str, BarcodeList],
    references: dict[str, KmerIndex],
    *,
    run_spot: float | None = None,
) -> Judged:
    """The call, the fit verdict, and the QC and library blocks for one set of reads."""
    call = classify(samples, lists, references)
    made = call.as_detail()
    measures = {
        "qc": qc(samples, made),
        "library": library(samples, made, references, tag_sets()),
    }
    return made, fit(samples, call, run_spot=run_spot), measures


def _judge_in_worker(samples: list[FileSample], run_spot: float | None) -> Judged:
    return judge(samples, _WORKER["lists"], _WORKER["references"], run_spot=run_spot)


class Pool:
    """Worker processes holding the lists and references; ``None`` means inline."""

    def __init__(
        self,
        lists: dict[str, BarcodeList],
        references: dict[str, KmerIndex],
        processes: int,
    ) -> None:
        self._executor = ProcessPoolExecutor(
            max_workers=processes, initializer=_start, initargs=(lists, references)
        )

    async def run[T](self, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._executor, functools.partial(fn, *args, **kwargs))

    async def judge(self, samples: list[FileSample], *, run_spot: float | None = None) -> Judged:
        return await self.run(_judge_in_worker, samples, run_spot)

    def close(self) -> None:
        self._executor.shutdown(wait=True, cancel_futures=True)

    def __enter__(self) -> Pool:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


async def parse[T](pool: Pool | None, fn: Callable[..., T], /, *args: Any, **kwargs: Any) -> T:
    """``fn(*args, **kwargs)``, in a worker when there is a pool."""
    if pool is None:
        return fn(*args, **kwargs)
    return await pool.run(fn, *args, **kwargs)


__all__ = ["Judged", "Pool", "judge", "parse"]
