"""Async gRPC Worker Loop with Fencing-Token Heartbeat Protection."""

import asyncio
import logging
from collections.abc import Callable, Coroutine

import grpc

from app.pb import broker_pb2, broker_pb2_grpc

logger = logging.getLogger(__name__)


async def default_execution_handler(job_id: str, payload_json: str) -> str:
    """Mock 3-step async sleep execution task."""
    logger.info("Executing 3-step job task for %s", job_id)
    # Step 1: Preprocessing
    await asyncio.sleep(0.05)
    # Step 2: Processing
    await asyncio.sleep(0.05)
    # Step 3: Finalizing
    await asyncio.sleep(0.05)
    return '{"status": "success", "steps_completed": 3}'


class WorkerLoop:
    """
    Manages continuous gRPC streaming connection to Go Broker's LeaseJobs,
    spawns background heartbeat tasks, and cancels execution immediately
    if lease_valid is reported as False.
    """

    def __init__(
        self,
        broker_addr: str = "localhost:50051",
        worker_id: str = "worker-1",
        heartbeat_interval_sec: float = 2.0,
        handler: Callable[[str, str], Coroutine] | None = None,
        stub: broker_pb2_grpc.BrokerServiceStub | None = None,
    ):
        self.broker_addr = broker_addr
        self.worker_id = worker_id
        self.heartbeat_interval = heartbeat_interval_sec
        self.handler = handler or default_execution_handler
        self._stub = stub
        self._channel = None
        self._running = False

    async def get_stub(self) -> broker_pb2_grpc.BrokerServiceStub:
        """Returns or creates the gRPC stub."""
        if self._stub:
            return self._stub
        if not self._channel:
            self._channel = grpc.aio.insecure_channel(self.broker_addr)
        return broker_pb2_grpc.BrokerServiceStub(self._channel)

    async def _heartbeat_loop(
        self,
        stub: broker_pb2_grpc.BrokerServiceStub,
        job_id: str,
        attempt_id: int,
        job_task: asyncio.Task,
    ):
        """
        Periodically sends Heartbeat(job_id, attempt_id) every 2 seconds.
        If heartbeat returns lease_valid == False, immediately cancels job_task.
        """
        try:
            while not job_task.done():
                await asyncio.sleep(self.heartbeat_interval)
                if job_task.done():
                    break

                req = broker_pb2.HeartbeatRequest(
                    job_id=job_id,
                    attempt_id=attempt_id,
                    worker_id=self.worker_id,
                )
                try:
                    resp = await stub.Heartbeat(req)
                    if not resp.lease_valid:
                        logger.warning(
                            "Lease lost or expired for job %s (attempt %d). Cancelling task.",
                            job_id,
                            attempt_id,
                        )
                        job_task.cancel()
                        break
                except Exception as e:
                    logger.warning("Heartbeat error for job %s: %s", job_id, e)
        except asyncio.CancelledError:
            pass

    async def process_lease(
        self,
        stub: broker_pb2_grpc.BrokerServiceStub,
        lease: broker_pb2.JobLease,
    ):
        """
        Executes a received lease:
        1. Launches execution task.
        2. Spawns concurrent background heartbeat task.
        3. Calls AckJob on success or NackJob on failure.
        """
        job_id = lease.job_id
        attempt_id = lease.attempt_id
        payload_json = lease.payload_json

        # 1. Spawn execution task
        job_task = asyncio.create_task(self.handler(job_id, payload_json))

        # 2. Spawn concurrent heartbeat loop
        hb_task = asyncio.create_task(self._heartbeat_loop(stub, job_id, attempt_id, job_task))

        try:
            result_json = await job_task
            # Cancel heartbeat monitor when task completes
            hb_task.cancel()

            # 3. Call AckJob on completion
            ack_req = broker_pb2.AckRequest(
                job_id=job_id,
                attempt_id=attempt_id,
                result_json=result_json,
            )
            await stub.AckJob(ack_req)
            logger.info("Successfully completed and acked job %s (attempt %d)", job_id, attempt_id)
        except asyncio.CancelledError:
            hb_task.cancel()
            logger.warning("Job %s was cancelled due to lost lease", job_id)
            # Stale lease lost: do not ack/nack since token is invalid
            raise
        except Exception as e:
            hb_task.cancel()
            logger.error("Unhandled exception processing job %s: %s", job_id, e)
            # Call NackJob on unhandled error
            nack_req = broker_pb2.NackRequest(
                job_id=job_id,
                attempt_id=attempt_id,
                error_message=str(e),
                retryable=True,
            )
            try:
                await stub.NackJob(nack_req)
            except Exception:
                pass

    async def run(self, max_leases: int | None = None):
        """
        Connects to Broker LeaseJobs stream and dispatches incoming leases.
        Can run indefinitely or exit after processing max_leases (useful for tests).
        """
        stub = await self.get_stub()
        identity = broker_pb2.WorkerIdentity(
            worker_id=self.worker_id,
            capabilities=["general", "mock_steps"],
            max_concurrency=1,
        )

        self._running = True
        processed = 0

        try:
            stream = stub.LeaseJobs(identity)
            async for lease in stream:
                if not self._running:
                    break
                await self.process_lease(stub, lease)
                processed += 1
                if max_leases is not None and processed >= max_leases:
                    break
        except asyncio.CancelledError:
            pass
        finally:
            self._running = False
            if self._channel:
                await self._channel.close()

    def stop(self):
        """Signal worker loop termination."""
        self._running = False
