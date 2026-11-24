"""Prometheus metrics.

HTTP metrics live in this process. Queue metrics are read from Redis on every scrape, so a
single API instance reports queue depth and job counters for the whole fleet of workers.
Each ``Metrics`` object owns a private registry, so tests can create many apps safely.
"""

from typing import Any

from prometheus_client import CollectorRegistry, Counter, Histogram
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from prometheus_client.registry import Collector

# Every counter is always published (as 0 until the first event). A series that first appears
# with a non-zero value makes Prometheus' rate()/increase() miss that first event, so e.g. the
# very first dead-lettered job would never trigger an alert.
JOB_EVENTS = ("enqueued", "succeeded", "retried", "dead", "reaped")


class QueueCollector(Collector):
    def __init__(self) -> None:
        self.snapshot: dict[str, Any] = {}

    def collect(self):
        depth = GaugeMetricFamily("taskforge_queue_depth", "Jobs per queue", labels=["queue"])
        for queue, value in self.snapshot.get("depth", {}).items():
            depth.add_metric([queue], value)
        yield depth

        jobs = CounterMetricFamily("taskforge_jobs", "Cumulative job events", labels=["event"])
        if self.snapshot:  # empty snapshot = Redis unreachable; publish nothing rather than fake zeros
            counters = self.snapshot.get("counters", {})
            for event in JOB_EVENTS:
                jobs.add_metric([event], counters.get(event, 0))
        yield jobs


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.http_requests = Counter(
            "taskforge_http_requests_total",
            "HTTP requests",
            ["method", "route", "status"],
            registry=self.registry,
        )
        self.http_latency = Histogram(
            "taskforge_http_request_duration_seconds",
            "HTTP request latency",
            ["method", "route"],
            buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
            registry=self.registry,
        )
        self.rate_limited = Counter(
            "taskforge_rate_limited_total", "Requests rejected by the rate limiter", registry=self.registry
        )
        self.queue = QueueCollector()
        self.registry.register(self.queue)
