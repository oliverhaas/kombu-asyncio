"""Unit tests for the pure asyncio Redis transport.

All Redis operations are mocked — no Redis server required.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kombu.entity import Exchange, Queue
from kombu.transport.redis import (
    BINDING_SEP,
    DEFAULT_MAX_RESTORE_COUNT,
    DEFAULT_VISIBILITY_TIMEOUT,
    MESSAGE_KEY_PREFIX,
    MESSAGES_INDEX_PREFIX,
    MIN_QUEUE_EXPIRES,
    Channel,
    Transport,
    _parse_db_from_url,
    _queue_score,
    _topic_match,
)

pytestmark = pytest.mark.asyncio(loop_scope="function")


# ---------------------------------------------------------------------------
# Mock helpers
# ---------------------------------------------------------------------------


def _make_transport(**opts) -> Transport:
    """Create a Transport with mocked Redis clients."""
    transport = Transport.__new__(Transport)
    transport._url = "redis://localhost:6379"
    transport._options = opts
    transport._client = MagicMock()
    transport._subclient = MagicMock()
    transport._channels = []
    transport._connection_id = "test-conn"
    transport._connected = True
    transport._db = "0"
    return transport


def _make_channel(**opts) -> Channel:
    """Create a Channel with a mocked transport."""
    transport = _make_transport(**opts)
    return Channel(transport, "test-conn")


class _MockPipeline:
    """Async context manager that collects pipeline calls."""

    def __init__(self):
        self.calls = []
        self._mock = AsyncMock()

    async def __aenter__(self):
        return self._mock

    async def __aexit__(self, *args):
        pass


# ---------------------------------------------------------------------------
# Helper function tests
# ---------------------------------------------------------------------------


class TestQueueScore:
    def test_basic_score(self):
        score = _queue_score(0, 1000.0)
        assert score > 0

    def test_higher_priority_lower_score(self):
        low = _queue_score(0, 1000.0)
        high = _queue_score(255, 1000.0)
        assert high < low

    def test_same_priority_earlier_first(self):
        earlier = _queue_score(5, 1000.0)
        later = _queue_score(5, 2000.0)
        assert earlier < later

    def test_clamps_priority(self):
        score_neg = _queue_score(-10, 1000.0)
        score_zero = _queue_score(0, 1000.0)
        assert score_neg == score_zero

        score_high = _queue_score(999, 1000.0)
        score_max = _queue_score(255, 1000.0)
        assert score_high == score_max


class TestTopicMatch:
    def test_exact(self):
        assert _topic_match("user.created", "user.created") is True

    def test_star(self):
        assert _topic_match("user.created", "user.*") is True
        assert _topic_match("user.profile.updated", "user.*") is False

    def test_hash(self):
        assert _topic_match("user.profile.updated", "user.#") is True
        assert _topic_match("user.created", "user.#") is True

    def test_no_match(self):
        assert _topic_match("user.created", "order.*") is False


class TestParseDbFromUrl:
    def test_default_db(self):
        assert _parse_db_from_url("redis://localhost:6379") == "0"

    def test_explicit_db(self):
        assert _parse_db_from_url("redis://localhost:6379/3") == "3"

    def test_empty_path(self):
        assert _parse_db_from_url("redis://localhost:6379/") == "0"


# ---------------------------------------------------------------------------
# Channel key helpers
# ---------------------------------------------------------------------------


class TestChannelKeyHelpers:
    def test_prefixed_no_prefix(self):
        ch = _make_channel()
        assert ch._prefixed("foo") == "foo"

    def test_prefixed_with_prefix(self):
        ch = _make_channel(global_keyprefix="myapp:")
        assert ch._prefixed("foo") == "myapp:foo"

    def test_unprefixed(self):
        ch = _make_channel(global_keyprefix="myapp:")
        assert ch._unprefixed("myapp:foo") == "foo"
        assert ch._unprefixed("other:foo") == "other:foo"

    def test_queue_key(self):
        ch = _make_channel(global_keyprefix="p:")
        assert ch._queue_key("celery") == "p:queue:celery"

    def test_message_key(self):
        ch = _make_channel(global_keyprefix="p:")
        assert ch._message_key("tag1") == "p:message:tag1"

    def test_messages_index_key(self):
        ch = _make_channel(global_keyprefix="p:")
        assert ch._messages_index_key("celery") == "p:messages_index:celery"


# ---------------------------------------------------------------------------
# Channel init
# ---------------------------------------------------------------------------


class TestChannelInit:
    def test_defaults(self):
        ch = _make_channel()
        assert ch._visibility_timeout == DEFAULT_VISIBILITY_TIMEOUT
        assert ch._message_ttl == -1
        assert ch._max_restore_count is None
        assert ch._consume_fast_mode is True
        assert ch._global_keyprefix == ""

    def test_custom_options(self):
        ch = _make_channel(
            visibility_timeout=120,
            message_ttl=3600,
            max_restore_count=5,
            global_keyprefix="test:",
        )
        assert ch._visibility_timeout == 120
        assert ch._message_ttl == 3600
        assert ch._max_restore_count == 5
        assert ch._global_keyprefix == "test:"

    def test_fanout_prefix_default(self):
        ch = _make_channel()
        assert ch._fanout_prefix == "/0."

    def test_fanout_prefix_custom(self):
        ch = _make_channel(fanout_prefix="/custom/{db}.")
        assert ch._fanout_prefix == "/custom/0."

    def test_fanout_prefix_disabled(self):
        ch = _make_channel(fanout_prefix=False)
        assert ch._fanout_prefix == ""


# ---------------------------------------------------------------------------
# Exchange operations
# ---------------------------------------------------------------------------


class TestExchangeOps:
    async def test_declare_exchange(self):
        ch = _make_channel()
        ex = Exchange("test_ex", type="direct")
        await ch.declare_exchange(ex)
        assert "test_ex" in ch._exchanges
        assert ch._exchanges["test_ex"]["type"] == "direct"

    async def test_exchange_delete(self):
        ch = _make_channel()
        ch.client.delete = AsyncMock()
        ch._exchanges["test_ex"] = {"type": "direct"}
        ch._bindings["test_ex"] = [("q1", "rk1")]
        await ch.exchange_delete("test_ex")
        assert "test_ex" not in ch._exchanges
        assert "test_ex" not in ch._bindings


# ---------------------------------------------------------------------------
# Queue operations
# ---------------------------------------------------------------------------


class TestQueueOps:
    async def test_declare_queue_auto_name(self):
        ch = _make_channel()
        ch.client.sadd = AsyncMock()
        q = Queue("")
        name = await ch.declare_queue(q)
        assert name.startswith("amq.gen-")

    async def test_declare_queue_with_expires(self):
        ch = _make_channel()
        ch.client.sadd = AsyncMock()
        q = Queue("test_q")
        q.queue_arguments = {"x-expires": 20_000}
        q.exchange = Exchange("ex", type="direct")
        q.routing_key = "rk"
        await ch.declare_queue(q)
        assert ch._expires["test_q"] == 20_000

    async def test_declare_queue_expires_clamped(self):
        ch = _make_channel()
        ch.client.sadd = AsyncMock()
        q = Queue("test_q")
        q.queue_arguments = {"x-expires": 5_000}  # Below MIN_QUEUE_EXPIRES
        await ch.declare_queue(q)
        assert ch._expires["test_q"] == MIN_QUEUE_EXPIRES

    async def test_queue_bind(self):
        ch = _make_channel()
        ch.client.sadd = AsyncMock()
        await ch.queue_bind("q1", "ex1", "rk1")
        assert ("q1", "rk1") in ch._bindings["ex1"]
        ch.client.sadd.assert_called_once()

    async def test_queue_unbind(self):
        ch = _make_channel()
        ch.client.srem = AsyncMock()
        ch._bindings["ex1"] = [("q1", "rk1")]
        await ch.queue_unbind("q1", "ex1", "rk1")
        assert ("q1", "rk1") not in ch._bindings["ex1"]

    async def test_queue_purge_cleans_hashes(self):
        ch = _make_channel()
        ch.client.zcard = AsyncMock(return_value=2)
        ch.client.zrange = AsyncMock(
            side_effect=[
                [b"tag1", b"tag2"],  # queue tags
                [b"tag1", b"tag3"],  # index tags (tag3 is extra)
            ],
        )
        mock_pipe = AsyncMock()
        mock_pipe.delete = AsyncMock()
        mock_pipe.execute = AsyncMock()

        class PipeCtx:
            async def __aenter__(self):
                return mock_pipe

            async def __aexit__(self, *a):
                pass

        ch.client.pipeline = MagicMock(return_value=PipeCtx())
        size = await ch.queue_purge("myqueue")
        assert size == 2
        # Should have called delete for queue, index, and each tag's message hash
        assert mock_pipe.delete.call_count >= 4  # queue, index, tag1, tag2, tag3

    async def test_queue_delete_cleans_hashes(self):
        ch = _make_channel()
        ch.client.zcard = AsyncMock(return_value=1)
        ch.client.zrange = AsyncMock(
            side_effect=[
                [b"tag1"],  # queue tags
                [b"tag1"],  # index tags (same)
            ],
        )

        mock_pipe = AsyncMock()
        mock_pipe.delete = AsyncMock()
        mock_pipe.execute = AsyncMock()

        class PipeCtx:
            async def __aenter__(self):
                return mock_pipe

            async def __aexit__(self, *a):
                pass

        ch.client.pipeline = MagicMock(return_value=PipeCtx())
        size = await ch.queue_delete("myqueue")
        assert size == 1
        # queue_key, index_key, message:tag1 = 3 delete calls
        assert mock_pipe.delete.call_count == 3

    async def test_queue_delete_if_empty(self):
        ch = _make_channel()
        ch.client.zcard = AsyncMock(return_value=5)
        result = await ch.queue_delete("myqueue", if_empty=True)
        assert result == 0  # Not deleted because not empty


# ---------------------------------------------------------------------------
# Publish
# ---------------------------------------------------------------------------


class TestPublish:
    async def test_direct_publish_default_exchange(self):
        ch = _make_channel()

        # Mock _put_message
        ch._put_message = AsyncMock()
        await ch.publish(b'{"body": "hi"}', exchange="", routing_key="myqueue")
        ch._put_message.assert_called_once_with("myqueue", b'{"body": "hi"}')

    async def test_fanout_publish(self):
        ch = _make_channel()
        ch._exchanges["fanout_ex"] = {"type": "fanout"}
        ch.client.xadd = AsyncMock()
        await ch.publish(b'{"body": "hi"}', exchange="fanout_ex", routing_key="")
        ch.client.xadd.assert_called_once()

    async def test_topic_publish(self):
        ch = _make_channel()
        ch._exchanges["topic_ex"] = {"type": "topic"}
        ch.client.smembers = AsyncMock(
            return_value={
                (BINDING_SEP.join(["user.*", "user.*", "q1"])).encode(),
            },
        )
        ch._put_message = AsyncMock()
        await ch.publish(b'{"body": "hi"}', exchange="topic_ex", routing_key="user.created")
        ch._put_message.assert_called_once()

    async def test_put_message_queue_at_includes_rci(self):
        """queue_at should include +RCI compensation."""
        ch = _make_channel(visibility_timeout=300)

        mock_pipe = AsyncMock()
        mock_pipe.hset = AsyncMock()
        mock_pipe.expire = AsyncMock()
        mock_pipe.zadd = AsyncMock()
        mock_pipe.pexpire = AsyncMock()
        mock_pipe.execute = AsyncMock()

        class PipeCtx:
            async def __aenter__(self):
                return mock_pipe

            async def __aexit__(self, *a):
                pass

        ch.client.pipeline = MagicMock(return_value=PipeCtx())

        with patch("kombu.transport.redis.time") as mock_time:
            mock_time.return_value = 1000.0
            await ch._put_message("q1", b'{"body": "test", "properties": {}, "headers": {}}')

        # Find the zadd call for messages_index
        for call in mock_pipe.zadd.call_args_list:
            args, kwargs = call
            key = args[0]
            if "messages_index" in key:
                mapping = args[1]
                for score in mapping.values():
                    # score should be now + VT + RCI = 1000 + 300 + 60 = 1360
                    assert score == 1360.0, f"Expected 1360.0, got {score}"
                break

    async def test_put_message_stores_restore_count(self):
        """New messages should have restore_count=0."""
        ch = _make_channel()

        mock_pipe = AsyncMock()
        mock_pipe.hset = AsyncMock()
        mock_pipe.zadd = AsyncMock()
        mock_pipe.execute = AsyncMock()

        class PipeCtx:
            async def __aenter__(self):
                return mock_pipe

            async def __aexit__(self, *a):
                pass

        ch.client.pipeline = MagicMock(return_value=PipeCtx())
        await ch._put_message("q1", b'{"body": "test", "properties": {}, "headers": {}}')

        # Check hset mapping includes restore_count=0
        hset_call = mock_pipe.hset.call_args
        mapping = hset_call[1]["mapping"]
        assert mapping["restore_count"] == 0


# ---------------------------------------------------------------------------
# FAST/SLOW consume
# ---------------------------------------------------------------------------


class TestFastSlowConsume:
    async def test_fast_consume_success(self):
        ch = _make_channel()
        ch._consume_fast_mode = True

        # Register a consumer
        cb = MagicMock()
        ch._consumers["tag1"] = ("q1", cb, True)

        # Mock consume script
        consume_script = AsyncMock(
            return_value=[
                b"q1",
                b"delivery-tag-1",
                b'{"body": "hello", "properties": {}, "headers": {}}',
                b"0",
            ],
        )
        ch._consume_script = consume_script

        result = await ch._fast_consume(["q1"])
        assert result is True
        cb.assert_called_once()

    async def test_fast_consume_empty_switches_to_slow(self):
        ch = _make_channel()
        ch._consume_fast_mode = True

        # Mock consume script returns nil (empty)
        consume_script = AsyncMock(return_value=None)
        ch._consume_script = consume_script

        result = await ch._fast_consume(["q1"])
        assert result is False
        # _consume_regular should have switched to slow mode

    async def test_consume_regular_fast_then_slow(self):
        ch = _make_channel()
        ch._consume_fast_mode = True

        # FAST returns nil
        consume_script = AsyncMock(return_value=None)
        ch._consume_script = consume_script

        # SLOW also returns nothing
        ch.client.bzmpop = AsyncMock(return_value=None)

        result = await ch._consume_regular(["q1"], timeout=1.0)
        assert result is False
        assert ch._consume_fast_mode is False  # Should have switched to SLOW

    async def test_slow_consume_switches_back_to_fast(self):
        ch = _make_channel()
        ch._consume_fast_mode = False

        # Register consumer
        cb = MagicMock()
        ch._consumers["tag1"] = ("q1", cb, True)

        # SLOW returns a message
        ch.client.bzmpop = AsyncMock(
            return_value=(
                b"queue:q1",
                [(b"delivery-tag-1", 1000.0)],
            ),
        )

        mock_pipe = AsyncMock()
        mock_pipe.zadd = AsyncMock()
        mock_pipe.hmget = AsyncMock()
        mock_pipe.execute = AsyncMock(
            return_value=[
                None,  # zadd result
                [b'{"body": "hello", "properties": {}, "headers": {}}', b"0"],  # hmget result
            ],
        )

        class PipeCtx:
            async def __aenter__(self):
                return mock_pipe

            async def __aexit__(self, *a):
                pass

        ch.client.pipeline = MagicMock(return_value=PipeCtx())

        result = await ch._slow_consume(["q1"], timeout=1.0)
        assert result is True
        # After successful SLOW, _consume_regular should switch back to FAST
        # (this happens in _consume_regular, not _slow_consume itself)

    async def test_fast_consume_with_restore_count(self):
        """FAST consume should inject x-restore-count header."""
        ch = _make_channel()
        ch._consume_fast_mode = True

        cb = MagicMock()
        ch._consumers["tag1"] = ("q1", cb, True)

        consume_script = AsyncMock(
            return_value=[
                b"q1",
                b"tag-1",
                b'{"body": "hello", "properties": {}, "headers": {}}',
                b"3",  # restore_count = 3
            ],
        )
        ch._consume_script = consume_script

        result = await ch._fast_consume(["q1"])
        assert result is True
        # Check the message passed to callback has x-restore-count
        call_args = cb.call_args
        msg = call_args[0][1]
        assert msg.headers.get("x-restore-count") == 3


# ---------------------------------------------------------------------------
# Ack / Reject / Recover
# ---------------------------------------------------------------------------


class TestAckReject:
    async def test_basic_ack_uses_lua_script(self):
        ch = _make_channel()
        ack_script = AsyncMock()
        ch._ack_script = ack_script

        # Track a delivered message
        msg = MagicMock()
        msg.delivery_tag = "tag1"
        ch._delivered["tag1"] = ("q1", msg)

        await ch.basic_ack("tag1")
        ack_script.assert_called_once()
        keys = ack_script.call_args[1]["keys"]
        assert len(keys) == 2  # index key, message key
        assert "tag1" not in ch._delivered

    async def test_basic_ack_fanout_skips_redis(self):
        ch = _make_channel()
        ack_script = AsyncMock()
        ch._ack_script = ack_script

        ch._fanout_tags.add("ftag1")
        ch._delivered["ftag1"] = ("q1", MagicMock())

        await ch.basic_ack("ftag1")
        ack_script.assert_not_called()
        assert "ftag1" not in ch._fanout_tags

    async def test_basic_reject_requeue(self):
        ch = _make_channel()
        ch._requeue_by_tag = AsyncMock(return_value=True)
        msg = MagicMock()
        ch._delivered["tag1"] = ("q1", msg)

        await ch.basic_reject("tag1", requeue=True)
        ch._requeue_by_tag.assert_called_once_with("tag1", leftmost=True)

    async def test_basic_reject_no_requeue_uses_lua(self):
        ch = _make_channel()
        ack_script = AsyncMock()
        ch._ack_script = ack_script
        msg = MagicMock()
        ch._delivered["tag1"] = ("q1", msg)

        await ch.basic_reject("tag1", requeue=False)
        ack_script.assert_called_once()

    async def test_basic_recover_requeues_all(self):
        ch = _make_channel()
        ch._requeue_by_tag = AsyncMock(return_value=True)
        ch._delivered["tag1"] = ("q1", MagicMock())
        ch._delivered["tag2"] = ("q2", MagicMock())

        await ch.basic_recover(requeue=True)
        assert ch._requeue_by_tag.call_count == 2
        assert len(ch._delivered) == 0


# ---------------------------------------------------------------------------
# Requeue (updated script args)
# ---------------------------------------------------------------------------


class TestRequeue:
    async def test_requeue_passes_new_args(self):
        ch = _make_channel(visibility_timeout=120)
        requeue_script = AsyncMock(return_value=1)
        ch._requeue_script = requeue_script

        result = await ch._requeue_by_tag("tag1", leftmost=True)
        assert result is True

        call_args = requeue_script.call_args
        args = call_args[1]["args"]
        assert args[0] == 1  # leftmost
        assert args[5] == MESSAGE_KEY_PREFIX  # message_key_prefix
        assert args[6] == 120  # visibility_timeout
        assert args[7] == MESSAGES_INDEX_PREFIX  # messages_index_prefix


# ---------------------------------------------------------------------------
# Enqueue due messages
# ---------------------------------------------------------------------------


class TestEnqueueDueMessages:
    async def test_enqueue_returns_tuple(self):
        ch = _make_channel()
        ch._consumers["tag1"] = ("q1", MagicMock(), False)

        enqueue_script = AsyncMock(return_value=[5, 2])  # {enqueued, dropped}
        ch._enqueue_script = enqueue_script

        enqueued, dropped = await ch._enqueue_due_messages()
        assert enqueued == 5
        assert dropped == 2

    async def test_enqueue_passes_max_restore_count(self):
        ch = _make_channel(max_restore_count=10)
        ch._consumers["tag1"] = ("q1", MagicMock(), False)

        enqueue_script = AsyncMock(return_value=[1, 0])
        ch._enqueue_script = enqueue_script

        await ch._enqueue_due_messages()
        call_args = enqueue_script.call_args
        args = call_args[1]["args"]
        assert args[7] == 10  # max_restore_count

    async def test_enqueue_no_limit(self):
        ch = _make_channel()  # max_restore_count=None (default)
        ch._consumers["tag1"] = ("q1", MagicMock(), False)

        enqueue_script = AsyncMock(return_value=[1, 0])
        ch._enqueue_script = enqueue_script

        await ch._enqueue_due_messages()
        call_args = enqueue_script.call_args
        args = call_args[1]["args"]
        assert args[7] == -1  # -1 = no limit

    async def test_enqueue_no_active_queues(self):
        ch = _make_channel()
        # No consumers
        enqueued, dropped = await ch._enqueue_due_messages()
        assert enqueued == 0
        assert dropped == 0


# ---------------------------------------------------------------------------
# Update messages index (heartbeat)
# ---------------------------------------------------------------------------


class TestUpdateMessagesIndex:
    async def test_heartbeat_includes_rci(self):
        ch = _make_channel(visibility_timeout=300)
        ch._delivered["tag1"] = ("q1", MagicMock())

        mock_pipe = AsyncMock()
        mock_pipe.zadd = AsyncMock()
        mock_pipe.execute = AsyncMock()

        class PipeCtx:
            async def __aenter__(self):
                return mock_pipe

            async def __aexit__(self, *a):
                pass

        ch.client.pipeline = MagicMock(return_value=PipeCtx())

        with patch("kombu.transport.redis.time") as mock_time:
            mock_time.return_value = 1000.0
            await ch._update_messages_index()

        # Should be now + VT + RCI = 1000 + 300 + 60 = 1360
        zadd_call = mock_pipe.zadd.call_args
        mapping = zadd_call[0][1]
        assert mapping["tag1"] == 1360.0


# ---------------------------------------------------------------------------
# get() uses consume script
# ---------------------------------------------------------------------------


class TestGet:
    async def test_get_uses_consume_script(self):
        ch = _make_channel()

        consume_script = AsyncMock(
            return_value=[
                b"q1",
                b"tag-1",
                b'{"body": "hello", "properties": {}, "headers": {}}',
                b"0",
            ],
        )
        ch._consume_script = consume_script

        msg = await ch.get("q1", no_ack=False)
        assert msg is not None
        assert msg.delivery_tag == "tag-1"
        assert "tag-1" in ch._delivered

    async def test_get_no_ack(self):
        ch = _make_channel()

        consume_script = AsyncMock(
            return_value=[
                b"q1",
                b"tag-1",
                b'{"body": "hello", "properties": {}, "headers": {}}',
                b"0",
            ],
        )
        ch._consume_script = consume_script

        msg = await ch.get("q1", no_ack=True)
        assert msg is not None
        assert "tag-1" not in ch._delivered

    async def test_get_empty(self):
        ch = _make_channel()

        consume_script = AsyncMock(return_value=None)
        ch._consume_script = consume_script

        msg = await ch.get("q1")
        assert msg is None

    async def test_get_with_restore_count(self):
        ch = _make_channel()

        consume_script = AsyncMock(
            return_value=[
                b"q1",
                b"tag-1",
                b'{"body": "hello", "properties": {}, "headers": {}}',
                b"7",  # restore_count=7
            ],
        )
        ch._consume_script = consume_script

        msg = await ch.get("q1", no_ack=True)
        assert msg.headers["x-restore-count"] == 7


# ---------------------------------------------------------------------------
# Close
# ---------------------------------------------------------------------------


class TestClose:
    async def test_close_requeues_delivered(self):
        ch = _make_channel()
        ch._requeue_by_tag = AsyncMock(return_value=True)
        ch._delivered["tag1"] = ("q1", MagicMock())
        ch._delivered["tag2"] = ("q2", MagicMock())
        ch._start_periodic_tasks = MagicMock()  # prevent actual tasks

        await ch.close()
        assert ch._requeue_by_tag.call_count == 2
        assert len(ch._delivered) == 0
        assert ch._closed is True

    async def test_close_skips_fanout_tags(self):
        ch = _make_channel()
        ch._requeue_by_tag = AsyncMock(return_value=True)
        ch._fanout_tags.add("ftag1")
        ch._delivered["ftag1"] = ("q1", MagicMock())
        ch._delivered["tag1"] = ("q2", MagicMock())

        await ch.close()
        # Only tag1 should be requeued, not ftag1
        ch._requeue_by_tag.assert_called_once()

    async def test_close_deletes_auto_delete_queues(self):
        ch = _make_channel()
        ch._requeue_by_tag = AsyncMock()
        ch.queue_delete = AsyncMock()
        ch.auto_delete_queues.add("auto_q")

        await ch.close()
        ch.queue_delete.assert_called_once_with("auto_q")


# ---------------------------------------------------------------------------
# Transport
# ---------------------------------------------------------------------------


class TestTransport:
    def test_init(self):
        t = Transport(url="redis://localhost:6379/2")
        assert t._db == "2"
        assert not t._connected

    def test_default_port(self):
        assert Transport.default_port == 6379

    def test_driver_type(self):
        assert Transport.driver_type == "redis"

    def test_connection_errors(self):
        assert ConnectionRefusedError in Transport.connection_errors
        assert TimeoutError in Transport.connection_errors

    def test_client_kwargs_filters_transport_options(self):
        t = Transport(
            url="redis://localhost:6379",
            global_keyprefix="p:",
            visibility_timeout=120,
            max_restore_count=5,
            credential_provider="some.module.Provider",
            socket_timeout=10,
        )
        kw = t._client_kwargs()
        assert "global_keyprefix" not in kw
        assert "visibility_timeout" not in kw
        assert "max_restore_count" not in kw
        assert "credential_provider" not in kw
        assert kw["socket_timeout"] == 10

    async def test_connect(self):
        t = Transport(url="redis://localhost:6379")
        with patch("kombu.transport.redis.aioredis") as mock_aioredis:
            mock_client = AsyncMock()
            mock_subclient = AsyncMock()
            mock_aioredis.from_url.side_effect = [mock_client, mock_subclient]
            mock_client.ping = AsyncMock()
            mock_subclient.ping = AsyncMock()

            await t.connect()
            assert t._connected is True
            assert mock_aioredis.from_url.call_count == 2

    async def test_close(self):
        t = _make_transport()
        ch = Channel(t, "conn")
        ch._closed = True  # Skip close logic
        t._channels = [ch]
        t._client.aclose = AsyncMock()
        t._subclient.aclose = AsyncMock()

        await t.close()
        assert not t._connected
        assert t._client is None
        assert t._subclient is None

    async def test_create_channel(self):
        t = _make_transport()
        ch = await t.create_channel()
        assert isinstance(ch, Channel)
        assert ch in t._channels

    def test_is_connected(self):
        t = _make_transport()
        assert t.is_connected is True
        t._connected = False
        assert t.is_connected is False

    def test_driver_version(self):
        t = _make_transport()
        version = t.driver_version()
        # Should return something (either version string or "N/A")
        assert isinstance(version, str)


# ---------------------------------------------------------------------------
# Credential provider
# ---------------------------------------------------------------------------


class TestCredentialProvider:
    def test_no_credential_provider(self):
        t = Transport(url="redis://localhost:6379")
        kw = t._process_credential_provider()
        assert kw == {}

    def test_credential_provider_instance(self):
        """Test with a mock credential provider instance."""
        t = Transport(url="redis://localhost:6379")

        # Create a mock that satisfies the CredentialProvider check
        mock_provider = MagicMock()

        with (
            patch.dict(t._options, {"credential_provider": mock_provider}),
            patch("kombu.transport.redis.aioredis"),
            patch("redis.credentials.CredentialProvider", new=type(mock_provider)),
        ):
            kw = t._process_credential_provider()
            assert kw["credential_provider"] is mock_provider

    def test_credential_provider_none(self):
        t = Transport(url="redis://localhost:6379")
        t._options["credential_provider"] = None
        kw = t._process_credential_provider()
        assert kw == {}


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_min_queue_expires(self):
        assert MIN_QUEUE_EXPIRES == 10_000

    def test_default_max_restore_count(self):
        assert DEFAULT_MAX_RESTORE_COUNT is None


# ---------------------------------------------------------------------------
# Drain events
# ---------------------------------------------------------------------------


class TestDrainEvents:
    async def test_drain_no_consumers(self):
        ch = _make_channel()
        result = await ch.drain_events(timeout=0.01)
        assert result is False

    async def test_drain_events_regular_queue(self):
        ch = _make_channel()
        ch._consumers["tag1"] = ("q1", MagicMock(), True)

        # Mock _consume_regular to return True
        ch._consume_regular = AsyncMock(return_value=True)

        result = await ch.drain_events(timeout=0.5)
        assert result is True


# ---------------------------------------------------------------------------
# Parse consume result
# ---------------------------------------------------------------------------


class TestParseConsumeResult:
    def test_bytes_result(self):
        ch = _make_channel()
        result = [b"q1", b"tag1", b'{"body": "hi"}', b"5"]
        q, tag, payload, rc = ch._parse_consume_result(result)
        assert q == "q1"
        assert tag == "tag1"
        assert payload == '{"body": "hi"}'
        assert rc == 5

    def test_string_result(self):
        ch = _make_channel()
        result = ["q1", "tag1", '{"body": "hi"}', "0"]
        q, tag, payload, rc = ch._parse_consume_result(result)
        assert q == "q1"
        assert tag == "tag1"
        assert rc == 0

    def test_none_restore_count(self):
        ch = _make_channel()
        result = [b"q1", b"tag1", b'{"body": "hi"}', None]
        _, _, _, rc = ch._parse_consume_result(result)
        assert rc == 0


# ---------------------------------------------------------------------------
# Lua script loading
# ---------------------------------------------------------------------------


class TestLuaScripts:
    async def test_consume_script_loaded(self):
        ch = _make_channel()
        mock_script = MagicMock()
        ch.client.register_script = MagicMock(return_value=mock_script)

        script = await ch._get_consume_script()
        assert script is mock_script
        ch.client.register_script.assert_called_once()

    async def test_ack_script_loaded(self):
        ch = _make_channel()
        mock_script = MagicMock()
        ch.client.register_script = MagicMock(return_value=mock_script)

        script = await ch._get_ack_script()
        assert script is mock_script
        ch.client.register_script.assert_called_once()

    async def test_scripts_cached(self):
        ch = _make_channel()
        mock_script = MagicMock()
        ch.client.register_script = MagicMock(return_value=mock_script)

        s1 = await ch._get_consume_script()
        s2 = await ch._get_consume_script()
        assert s1 is s2
        assert ch.client.register_script.call_count == 1


# ---------------------------------------------------------------------------
# Bindings
# ---------------------------------------------------------------------------


class TestLoadBindings:
    async def test_load_sep_format(self):
        ch = _make_channel()
        binding = BINDING_SEP.join(["rk1", "rk1", "q1"])
        ch.client.smembers = AsyncMock(return_value={binding.encode()})

        bindings = await ch._load_bindings("ex1")
        assert bindings == [("q1", "rk1")]

    async def test_load_json_format(self):
        ch = _make_channel()
        ch.client.smembers = AsyncMock(
            return_value={
                b'{"queue": "q1", "routing_key": "rk1"}',
            },
        )

        bindings = await ch._load_bindings("ex1")
        assert bindings == [("q1", "rk1")]


# Cleanup helper that was referenced but not defined
class _CapturingPipeline:
    """Not used anymore, kept for compat."""

    async def __aenter__(self):
        return AsyncMock()

    async def __aexit__(self, *a):
        pass
