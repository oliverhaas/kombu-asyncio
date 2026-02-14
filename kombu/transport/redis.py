"""Pure asyncio Redis transport for Kombu.

This transport uses redis.asyncio (redis-py async) for all operations.
No sync code, no Hub, direct asyncio integration.

Connection String
=================
Connection string has the following format:

.. code-block::

    redis://[USER:PASSWORD@]REDIS_ADDRESS[:PORT][/DB]
    rediss://[USER:PASSWORD@]REDIS_ADDRESS[:PORT][/DB]

Transport Options
=================
* ``socket_timeout``: Socket timeout in seconds
* ``socket_connect_timeout``: Socket connection timeout in seconds
* ``health_check_interval``: Health check interval for connections
* ``max_connections``: Maximum connections in pool
"""

from __future__ import annotations

import asyncio
import re
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

try:
    import redis.asyncio as aioredis
except ImportError:
    aioredis = None  # type: ignore

from kombu.log import get_logger
from kombu.message import Message
from kombu.transport.base import Transport as BaseTransport
from kombu.utils.json import dumps as json_dumps
from kombu.utils.json import loads as json_loads

if TYPE_CHECKING:
    from kombu.entity import Exchange, Queue

__all__ = ("Channel", "Transport")

logger = get_logger("kombu.transport.redis")

# Default exchange name
DEFAULT_EXCHANGE = ""

# Key prefixes for Redis
QUEUE_KEY_PREFIX = "_kombu.binding."
UNACKED_KEY_PREFIX = "_kombu.unacked."


def _queue_key(queue: str) -> str:
    """Get Redis key for a queue."""
    return queue


def _binding_key(exchange: str) -> str:
    """Get Redis key for storing exchange bindings."""
    return f"{QUEUE_KEY_PREFIX}{exchange}"


class Channel:
    """A virtual channel for Redis operations.

    Each channel manages its own consumers and message delivery.
    """

    def __init__(self, transport: Transport, connection_id: str):
        self._transport = transport
        self._connection_id = connection_id
        self._channel_id = str(uuid.uuid4())
        self._consumers: dict[str, tuple[str, Callable, bool]] = {}  # tag -> (queue, callback, no_ack)
        self._exchanges: dict[str, dict] = {}  # Exchange metadata
        self._bindings: dict[str, list[tuple[str, str]]] = {}  # exchange -> [(queue, routing_key)]
        self._closed = False

        # For no-ack consumers
        self.no_ack_consumers: set[str] | None = set()

        # Unacked messages (delivery_tag -> (queue, message_data))
        self._unacked: dict[str, tuple[str, bytes]] = {}
        self._delivery_tag_counter = 0

    @property
    def client(self) -> aioredis.Redis:
        """Get the Redis client."""
        return self._transport._client

    def _next_delivery_tag(self) -> str:
        """Generate next delivery tag."""
        self._delivery_tag_counter += 1
        return f"{self._channel_id}.{self._delivery_tag_counter}"

    async def close(self) -> None:
        """Close the channel."""
        if self._closed:
            return
        self._closed = True

        # Requeue unacked messages
        for delivery_tag, (queue, data) in self._unacked.items():
            try:
                await self.client.lpush(_queue_key(queue), data)
            except Exception:
                logger.warning(
                    "Failed to requeue message %s to %s",
                    delivery_tag,
                    queue,
                )
        self._unacked.clear()
        self._consumers.clear()

    # Exchange operations

    async def declare_exchange(self, exchange: Exchange) -> None:
        """Declare an exchange.

        For Redis, we just store metadata about the exchange.
        """
        self._exchanges[exchange.name] = {
            "type": exchange.type,
            "durable": exchange.durable,
            "auto_delete": exchange.auto_delete,
            "arguments": exchange.arguments,
        }

    async def exchange_delete(self, exchange: str) -> None:
        """Delete an exchange."""
        self._exchanges.pop(exchange, None)
        self._bindings.pop(exchange, None)
        # Delete binding keys from Redis
        await self.client.delete(_binding_key(exchange))

    # Queue operations

    async def declare_queue(self, queue: Queue) -> str:
        """Declare a queue.

        For Redis, queues are created on first use.
        Returns the queue name.
        """
        name = queue.name or f"amq.gen-{uuid.uuid4()}"
        queue.name = name

        # Store binding if exchange is specified
        if queue.exchange:
            await self.queue_bind(
                queue=name,
                exchange=queue.exchange.name,
                routing_key=queue.routing_key,
            )
        return name

    async def queue_bind(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: dict | None = None,
    ) -> None:
        """Bind a queue to an exchange."""
        if exchange not in self._bindings:
            self._bindings[exchange] = []

        binding = (queue, routing_key or queue)
        if binding not in self._bindings[exchange]:
            self._bindings[exchange].append(binding)

        # Store binding in Redis for persistence
        binding_data = json_dumps({"queue": queue, "routing_key": routing_key})
        await self.client.sadd(_binding_key(exchange), binding_data)

    async def queue_unbind(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: dict | None = None,
    ) -> None:
        """Unbind a queue from an exchange."""
        if exchange in self._bindings:
            binding = (queue, routing_key or queue)
            if binding in self._bindings[exchange]:
                self._bindings[exchange].remove(binding)

        # Remove from Redis
        binding_data = json_dumps({"queue": queue, "routing_key": routing_key})
        await self.client.srem(_binding_key(exchange), binding_data)

    async def queue_purge(self, queue: str) -> int:
        """Purge all messages from a queue."""
        key = _queue_key(queue)
        length = await self.client.llen(key)
        await self.client.delete(key)
        return length

    async def queue_delete(
        self,
        queue: str,
        if_unused: bool = False,
        if_empty: bool = False,
    ) -> int:
        """Delete a queue."""
        key = _queue_key(queue)

        if if_empty:
            length = await self.client.llen(key)
            if length > 0:
                return 0

        length = await self.client.llen(key)
        await self.client.delete(key)

        # Remove from all exchange bindings
        for exchange, bindings in list(self._bindings.items()):
            self._bindings[exchange] = [(q, rk) for q, rk in bindings if q != queue]

        return length

    # Message operations

    async def publish(
        self,
        message: bytes,
        exchange: str,
        routing_key: str,
        **kwargs: Any,
    ) -> None:
        """Publish a message to an exchange.

        Routing depends on exchange type:
        - direct: Route to queue matching routing_key
        - fanout: Route to all bound queues via pub/sub
        - topic: Route to queues with matching pattern
        """
        exchange = exchange or DEFAULT_EXCHANGE
        exchange_meta = self._exchanges.get(exchange, {"type": "direct"})
        exchange_type = exchange_meta.get("type", "direct")

        if exchange_type == "fanout":
            await self._fanout_publish(exchange, message)
        elif exchange_type == "topic":
            await self._topic_publish(exchange, routing_key, message)
        else:
            await self._direct_publish(exchange, routing_key, message)

    async def _load_bindings(self, exchange: str) -> list[tuple[str, str]]:
        """Load bindings for an exchange from Redis.

        Always loads from Redis since other connections may add or remove
        bindings (e.g. ephemeral reply queues created by CLI invocations).
        """
        binding_key = _binding_key(exchange)
        members = await self.client.smembers(binding_key)
        bindings = []
        for member in members:
            if isinstance(member, bytes):
                member = member.decode()
            data = json_loads(member)
            bindings.append((data["queue"], data.get("routing_key", "")))

        return bindings

    async def _direct_publish(
        self,
        exchange: str,
        routing_key: str,
        message: bytes,
    ) -> None:
        """Publish to direct exchange."""
        if exchange:
            bindings = await self._load_bindings(exchange)
            for queue, rk in bindings:
                if rk == routing_key:
                    await self.client.lpush(_queue_key(queue), message)
        else:
            # Default exchange: routing_key is the queue name
            await self.client.lpush(_queue_key(routing_key), message)

    async def _fanout_publish(
        self,
        exchange: str,
        message: bytes,
    ) -> None:
        """Publish to fanout exchange."""
        # Publish to all bound queues
        bindings = await self._load_bindings(exchange)
        for queue, _ in bindings:
            await self.client.lpush(_queue_key(queue), message)
        # Also publish to pub/sub for real-time consumers
        await self.client.publish(exchange, message)

    async def _topic_publish(
        self,
        exchange: str,
        routing_key: str,
        message: bytes,
    ) -> None:
        """Publish to topic exchange with pattern matching."""
        bindings = await self._load_bindings(exchange)
        for queue, pattern in bindings:
            if self._topic_match(routing_key, pattern):
                await self.client.lpush(_queue_key(queue), message)

    def _topic_match(self, routing_key: str, pattern: str) -> bool:
        """Match routing key against topic pattern.

        Supports:
        - * matches exactly one word
        - # matches zero or more words
        """
        # Convert AMQP pattern to regex
        regex_pattern = pattern.replace(".", r"\.")
        regex_pattern = regex_pattern.replace("*", r"[^.]+")
        regex_pattern = regex_pattern.replace("#", r".*")
        regex_pattern = f"^{regex_pattern}$"
        return bool(re.match(regex_pattern, routing_key))

    async def get(
        self,
        queue: str,
        no_ack: bool = False,
        accept: set[str] | None = None,
    ) -> Message | None:
        """Get a single message from a queue.

        Returns None if queue is empty.
        """
        key = _queue_key(queue)
        data = await self.client.rpop(key)
        if data is None:
            return None

        return self._create_message(queue, data, no_ack, accept)

    async def basic_consume(
        self,
        queue: str,
        callback: Callable[[Message], Any],
        consumer_tag: str | None = None,
        no_ack: bool = False,
    ) -> str:
        """Register a consumer for a queue."""
        if consumer_tag is None:
            consumer_tag = str(uuid.uuid4())

        self._consumers[consumer_tag] = (queue, callback, no_ack)

        if no_ack and self.no_ack_consumers is not None:
            self.no_ack_consumers.add(consumer_tag)

        return consumer_tag

    async def basic_cancel(self, consumer_tag: str) -> None:
        """Cancel a consumer."""
        self._consumers.pop(consumer_tag, None)
        if self.no_ack_consumers is not None:
            self.no_ack_consumers.discard(consumer_tag)

    async def drain_events(self, timeout: float | None = None) -> bool:
        """Wait for and deliver messages to consumers.

        Returns True if a message was delivered, False on timeout.
        """
        if not self._consumers:
            await asyncio.sleep(0.1)
            return False

        # Get all queue keys
        queues = list({q for q, _, _ in self._consumers.values()})
        queue_keys = [_queue_key(q) for q in queues]

        # Use BRPOP for blocking wait
        effective_timeout = timeout if timeout else 1.0
        result = await self.client.brpop(queue_keys, timeout=effective_timeout)

        if result is None:
            return False

        queue_key, data = result
        # Find the queue name from the key
        queue = queue_key.decode() if isinstance(queue_key, bytes) else queue_key

        # Find and call the callback
        for tag, (q, callback, no_ack) in self._consumers.items():
            if _queue_key(q) == queue:
                message = self._create_message(q, data, no_ack)
                await self._deliver_message(callback, message)
                return True

        return False

    async def _deliver_message(
        self,
        callback: Callable[[Message], Any],
        message: Message,
    ) -> None:
        """Deliver a message to a callback."""
        # Decode the body for the callback
        try:
            body = message.decode()
        except Exception:
            body = message.body

        result = callback(body, message)
        if asyncio.iscoroutine(result):
            await result

    def _create_message(
        self,
        queue: str,
        data: bytes,
        no_ack: bool = False,
        accept: set[str] | None = None,
    ) -> Message:
        """Create a Message object from raw data."""
        try:
            # Parse the message envelope
            payload = json_loads(data)
            body = payload.get("body", data)
            content_type = payload.get("content-type", "application/json")
            content_encoding = payload.get("content-encoding", "utf-8")
            properties = payload.get("properties", {})
            headers = payload.get("headers", {})

            # Convert body back to bytes for proper deserialization
            if isinstance(body, str):
                body = body.encode(content_encoding)
            elif isinstance(body, dict) or isinstance(body, list):
                # Body is already decoded JSON - re-encode it
                body = json_dumps(body).encode("utf-8")
        except (ValueError, TypeError):
            # Raw message, no envelope
            body = data
            content_type = "application/data"
            content_encoding = "binary"
            properties = {}
            headers = {}

        delivery_tag = self._next_delivery_tag()

        if not no_ack:
            # Store for acknowledgment
            self._unacked[delivery_tag] = (queue, data)

        return Message(
            body=body,
            delivery_tag=delivery_tag,
            content_type=content_type,
            content_encoding=content_encoding,
            delivery_info={
                "exchange": "",
                "routing_key": queue,
            },
            properties=properties,
            headers=headers,
            accept=accept,
            channel=self,
        )

    # Acknowledgment operations

    async def basic_ack(self, delivery_tag: str, multiple: bool = False) -> None:
        """Acknowledge a message."""
        if multiple:
            # Ack all messages up to and including this one
            tags_to_ack = []
            for tag in self._unacked:
                tags_to_ack.append(tag)
                if tag == delivery_tag:
                    break
            for tag in tags_to_ack:
                self._unacked.pop(tag, None)
        else:
            self._unacked.pop(delivery_tag, None)

    async def basic_reject(self, delivery_tag: str, requeue: bool = True) -> None:
        """Reject a message."""
        entry = self._unacked.pop(delivery_tag, None)
        if entry and requeue:
            queue, data = entry
            # Requeue at the end of the queue
            await self.client.rpush(_queue_key(queue), data)

    async def basic_recover(self, requeue: bool = True) -> None:
        """Recover unacknowledged messages."""
        if requeue:
            for delivery_tag, (queue, data) in list(self._unacked.items()):
                await self.client.rpush(_queue_key(queue), data)
        self._unacked.clear()


class Transport(BaseTransport):
    """Pure asyncio Redis transport.

    Uses redis.asyncio for all operations.
    """

    Channel = Channel
    default_port = 6379

    driver_type = "redis"
    driver_name = "redis"

    connection_errors = BaseTransport.connection_errors + (
        ConnectionRefusedError,
        TimeoutError,
    )
    channel_errors = BaseTransport.channel_errors

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        **options: Any,
    ):
        if aioredis is None:
            raise ImportError("redis package is required for Redis transport. Install it with: pip install redis")
        self._url = url
        self._options = options
        self._client: aioredis.Redis | None = None
        self._channels: list[Channel] = []
        self._connection_id = str(uuid.uuid4())
        self._connected = False

    async def connect(self) -> None:
        """Establish connection to Redis."""
        if self._connected:
            return

        self._client = aioredis.from_url(
            self._url,
            decode_responses=False,
            **self._options,
        )

        # Test connection
        await self._client.ping()
        self._connected = True
        logger.debug("Connected to Redis at %s", self._url)

    async def close(self) -> None:
        """Close the transport and all channels."""
        # Close all channels
        for channel in self._channels:
            await channel.close()
        self._channels.clear()

        # Close Redis client
        if self._client:
            await self._client.aclose()
            self._client = None

        self._connected = False

    async def create_channel(self) -> Channel:
        """Create a new channel."""
        if not self._connected:
            await self.connect()

        channel = Channel(self, self._connection_id)
        self._channels.append(channel)
        return channel

    @property
    def is_connected(self) -> bool:
        """Check if transport is connected."""
        return self._connected and self._client is not None
