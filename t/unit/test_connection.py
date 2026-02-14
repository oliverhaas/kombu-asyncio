"""Tests for kombu.connection - async Connection."""

from __future__ import annotations

import pytest

from kombu import Connection


class test_Connection:
    """Tests for Connection class."""

    def test_init_redis(self):
        conn = Connection("redis://localhost:6379")
        assert conn._url == "redis://localhost:6379"
        assert conn._scheme == "redis"
        assert not conn.is_connected
        assert conn.transport is None

    def test_init_memory(self):
        conn = Connection("memory://")
        assert conn._scheme == "memory"

    def test_init_unsupported_scheme(self):
        with pytest.raises(ValueError, match="Unsupported transport scheme"):
            Connection("ftp://localhost")

    def test_init_default_url(self):
        conn = Connection()
        assert conn._url == "redis://localhost:6379"

    def test_repr(self):
        conn = Connection("memory://")
        r = repr(conn)
        assert "memory://" in r
        assert "connected=False" in r

    def test_connected_alias(self):
        conn = Connection("memory://")
        assert conn.connected is conn.is_connected

    async def test_connect(self):
        conn = Connection("memory://")
        result = await conn.connect()
        assert result is conn
        assert conn.is_connected
        assert conn.transport is not None
        await conn.close()

    async def test_close(self):
        conn = Connection("memory://")
        await conn.connect()
        assert conn.is_connected
        await conn.close()
        assert not conn.is_connected
        assert conn.transport is None

    async def test_close_idempotent(self):
        conn = Connection("memory://")
        await conn.connect()
        await conn.close()
        await conn.close()  # Should not raise

    async def test_channel(self):
        conn = Connection("memory://")
        channel = await conn.channel()
        assert channel is not None
        assert conn.is_connected  # Auto-connected
        await conn.close()

    async def test_default_channel(self):
        conn = Connection("memory://")
        ch1 = await conn.default_channel()
        ch2 = await conn.default_channel()
        assert ch1 is ch2  # Same instance
        await conn.close()

    async def test_context_manager(self):
        async with Connection("memory://") as conn:
            assert conn.is_connected
        assert not conn.is_connected

    async def test_release_alias(self):
        conn = Connection("memory://")
        await conn.connect()
        await conn.release()
        assert not conn.is_connected

    def test_clone(self):
        conn = Connection("memory://", transport_options={"foo": "bar"})
        cloned = conn.clone()
        assert cloned._url == conn._url
        assert cloned._transport_options == conn._transport_options
        assert cloned is not conn

    def test_clone_override(self):
        conn = Connection("memory://")
        cloned = conn.clone(hostname="redis://localhost")
        assert cloned._url == "redis://localhost"

    async def test_ensure_connection(self):
        conn = Connection("memory://")
        result = await conn.ensure_connection(max_retries=3)
        assert result is conn
        assert conn.is_connected
        await conn.close()

    async def test_drain_events_timeout(self):
        async with Connection("memory://") as conn:
            # With no consumers and a timeout, should raise TimeoutError
            with pytest.raises(TimeoutError):
                await conn.drain_events(timeout=0.01)

    def test_producer_factory(self):
        conn = Connection("memory://")
        producer = conn.Producer()
        assert producer is not None
        assert producer._connection is conn

    def test_consumer_factory(self):
        from kombu import Queue

        conn = Connection("memory://")
        queue = Queue("test_q")
        consumer = conn.Consumer([queue])
        assert consumer is not None
        assert consumer._connection is conn

    def test_simple_queue_factory(self):
        conn = Connection("memory://")
        sq = conn.SimpleQueue("test")
        assert sq is not None
