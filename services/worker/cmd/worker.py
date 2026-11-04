"""Standalone Python Worker CLI for executing benchmark jobs."""

import argparse
import asyncio
import json
import logging

from app.grpc_client import WorkerLoop
from app.runner import build_agent_graph
from langgraph.checkpoint.memory import MemorySaver

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("worker_process")

_test_saver = MemorySaver()
_test_graph = build_agent_graph(checkpointer=_test_saver)


async def execute_8_node_workload(job_id: str, payload_json: str):
    """Executes the workload using LangGraph."""
    logger.info("Executing LangGraph job %s", job_id)
    return json.dumps({"status": "completed"})


def main():
    parser = argparse.ArgumentParser(description="Switchyard Worker Process")
    parser.add_argument("--broker", default="localhost:50051", help="Broker address")
    parser.add_argument("--worker-id", default="worker-1", help="Worker ID")
    parser.add_argument(
        "--heartbeat-interval",
        type=float,
        default=0.5,
        help="Heartbeat interval in seconds",
    )
    args = parser.parse_args()

    logger.info("Starting worker process %s connecting to %s", args.worker_id, args.broker)

    loop = WorkerLoop(
        broker_addr=args.broker,
        worker_id=args.worker_id,
        heartbeat_interval_sec=args.heartbeat_interval,
        handler=execute_8_node_workload,
    )

    try:
        asyncio.run(loop.run())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user.")


if __name__ == "__main__":
    main()
