"""Built-in transports - Pure asyncio version.

Currently supported transports:
- valkey/redis: Valkey/Redis using valkey.asyncio or redis.asyncio
- memory: In-memory transport using asyncio.Queue
- filesystem: File-system based transport using aiofiles
"""

from .filesystem import Transport as FilesystemTransport
from .memory import Transport as MemoryTransport
from .redis import Transport as RedisTransport

TRANSPORT_ALIASES = {
    "valkey": "kombu.transport.redis:Transport",
    "valkeys": "kombu.transport.redis:Transport",
    "redis": "kombu.transport.redis:Transport",
    "rediss": "kombu.transport.redis:Transport",
    "memory": "kombu.transport.memory:Transport",
    "filesystem": "kombu.transport.filesystem:Transport",
}

__all__ = (
    "TRANSPORT_ALIASES",
    "FilesystemTransport",
    "MemoryTransport",
    "RedisTransport",
)
