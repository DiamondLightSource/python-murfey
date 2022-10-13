from __future__ import annotations

import asyncio
import inspect
from unittest import mock

import pytest

from murfey.util.state import State


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


def test_calling_async_methods_synchronously():
    s = State()
    return_value = s.aupdate("key", "value")
    assert inspect.isawaitable(return_value)
    assert len(s) == 0
    asyncio.run(return_value)
    assert len(s) == 1

    return_value = s.delete("key")
    assert inspect.isawaitable(return_value)
    assert len(s) == 1
    asyncio.run(return_value)
    assert len(s) == 0


def test_calling_sync_methods_asynchronously():
    s = State()

    async def set_value():
        s["key"] = "value"

    async def delete_value():
        del s["key"]

    with pytest.raises(RuntimeError, match="async.*update.*instead"):
        asyncio.run(set_value())
    assert not s

    s["key"] = "value"
    assert s
    with pytest.raises(RuntimeError, match="async.*delete.*instead"):
        asyncio.run(delete_value())
    assert s


def test_state_object_supports_multiple_non_async_listeners():
    s = State()
    listener = mock.Mock()
    s.subscribe(listener)
    s.subscribe(listener)
    assert "2" in repr(s)

    s["attribute"] = mock.sentinel.value

    assert listener.call_count == 2
    listener.assert_has_calls([mock.call("attribute", mock.sentinel.value)] * 2)


def test_state_object_notifies_listeners_on_synchronous_change():
    # Test with both sync and async subscribers
    s = State()
    assert "0" in repr(s)

    sync_listener = mock.Mock()
    s.subscribe(sync_listener)
    sync_listener.assert_not_called()
    async_listener = mock.AsyncMock()
    s.subscribe(async_listener)
    async_listener.assert_not_called()
    assert "2" in repr(s)
    assert "key" not in repr(s)

    s["key"] = mock.sentinel.value
    sync_listener.assert_called_once_with("key", mock.sentinel.value)
    async_listener.assert_called_once_with("key", mock.sentinel.value)
    async_listener.assert_awaited()
    assert "key" in repr(s)

    sync_listener.reset_mock()
    async_listener.reset_mock()
    sync_listener.assert_not_called()
    async_listener.assert_not_called()
    s["key"] = mock.sentinel.value2
    sync_listener.assert_called_once_with("key", mock.sentinel.value2)
    async_listener.assert_called_once_with("key", mock.sentinel.value2)
    async_listener.assert_awaited()

    sync_listener.reset_mock()
    async_listener.reset_mock()
    assert s["key"] == mock.sentinel.value2
    # Dictionary access should not notify
    sync_listener.assert_not_called()
    async_listener.assert_not_called()

    sync_listener.reset_mock()
    async_listener.reset_mock()
    del s["key"]
    sync_listener.assert_called_once_with("key", None)
    async_listener.assert_called_once_with("key", None)
    async_listener.assert_awaited()


def test_state_object_notifies_listeners_on_asynchronous_change():
    # Test with both sync and async subscribers
    s = State()
    assert "0" in repr(s)

    sync_listener = mock.Mock()
    s.subscribe(sync_listener)
    sync_listener.assert_not_called()
    async_listener = mock.AsyncMock()
    s.subscribe(async_listener)
    async_listener.assert_not_called()
    assert "2" in repr(s)
    assert "key" not in repr(s)

    async def set_value():
        await s.aupdate("key", mock.sentinel.value)

    async def set_value2():
        await s.aupdate("key", mock.sentinel.value2)

    async def delete_value():
        await s.delete("key")

    asyncio.run(set_value())
    sync_listener.assert_called_once_with(
        "key", mock.sentinel.value, message="state-update"
    )
    async_listener.assert_called_once_with(
        "key", mock.sentinel.value, message="state-update"
    )
    async_listener.assert_awaited()
    assert "key" in repr(s)

    sync_listener.reset_mock()
    async_listener.reset_mock()
    sync_listener.assert_not_called()
    async_listener.assert_not_called()
    asyncio.run(set_value2())
    # sync_listener.assert_called_once_with("key", mock.sentinel.value2, message="state-update-partial")
    # async_listener.assert_called_once_with("key", mock.sentinel.value2)
    # async_listener.assert_awaited()

    sync_listener.reset_mock()
    async_listener.reset_mock()
    asyncio.run(delete_value())
    sync_listener.assert_called_once_with("key", None)
    async_listener.assert_called_once_with("key", None)
    async_listener.assert_awaited()
