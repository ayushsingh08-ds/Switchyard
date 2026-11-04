from app.pb import broker_pb2, broker_pb2_grpc


def test_proto_imports():
    """Verify that generated proto messages and service stubs can be instantiated."""
    req = broker_pb2.EnqueueRequest(
        tenant_id="tenant-123",
        payload_json='{"task": "embed"}',
        budget_usd=1.50
    )
    assert req.tenant_id == "tenant-123"
    assert req.budget_usd == 1.50
    assert hasattr(broker_pb2_grpc, "BrokerServiceStub")
