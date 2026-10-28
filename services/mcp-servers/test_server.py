from server import list_servers


def test_list_servers():
    assert list_servers() == ["default"]
