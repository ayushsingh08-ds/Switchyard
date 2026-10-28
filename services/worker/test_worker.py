from worker import get_worker_status


def test_get_worker_status():
    assert get_worker_status() == "ok"
