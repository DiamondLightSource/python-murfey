from __future__ import annotations

from unittest import mock

from murfey.server.state import State


def test_default_state_behaves_like_empty_dictionary():
    s = State()
    assert s == {}
    assert dict(s) == {}
    assert len(s) == 0
    assert not s


def test_state_object_behaves_like_a_dictionary():
    s = State()

    assert s.get("key") is None
    assert "key" not in s
    assert len(s) == 0
    assert not s

    s["key"] = "value"

    assert s["key"] == "value"
    assert "key" in s
    assert "notkey" not in s
    assert len(s) == 1
    assert s

    s["key"] = "newvalue"

    assert s["key"] == "newvalue"
    assert "key" in s
    assert len(s) == 1
    assert s


def test_state_object_notifies_listeners_on_change():
    s = State()
    assert "0" in repr(s)

    listener = mock.Mock()
    s.subscribe(listener)
    listener.assert_not_called()
    assert "1" in repr(s)
    assert "key" not in repr(s)

    s["key"] = mock.sentinel.value
    listener.assert_called_once_with("key", mock.sentinel.value)
    assert "key" in repr(s)

    listener.reset_mock()
    listener.assert_not_called()
    s["key"] = mock.sentinel.value2
    listener.assert_called_once_with("key", mock.sentinel.value2)

    listener.reset_mock()
    listener.assert_not_called()
    assert s["key"] == mock.sentinel.value2
    # Dictionary access should not notify
    listener.assert_not_called()

    listener.reset_mock()
    listener.assert_not_called()
    del s["key"]
    listener.assert_called_once_with("key", None)


def test_state_object_supports_multiple_listeners():
    s = State()
    listener = mock.Mock()
    s.subscribe(listener)
    s.subscribe(listener)
    assert "2" in repr(s)

    s["attribute"] = mock.sentinel.value

    assert listener.call_count == 2
    listener.assert_has_calls([mock.call("attribute", mock.sentinel.value)] * 2)
