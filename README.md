# kombu-asyncio

Asyncio-native rewrite of [Kombu](https://github.com/celery/kombu), the battle-tested messaging library that powers [Celery](https://github.com/celery/celery) — one of the most widely used distributed task systems in the Python ecosystem. This is the messaging layer for [celery-asyncio](https://github.com/oliverhaas/celery-asyncio).

This project is **exploratory and experimental**. It is not affiliated with or endorsed by the Celery project. If you're looking for a production-ready messaging library, use the original [Kombu](https://github.com/celery/kombu).

This package exists as a standalone library for compatibility with packages that import from `kombu` directly (e.g. Celery Flower). Long-term it will be merged into celery-asyncio.

## Overview

- Ground-up asyncio rewrite — all operations are `async`/`await`, no sync API
- Transports: Redis, AMQP (via aio-pika), Memory, Filesystem
- Exchange types: direct, fanout, topic (emulated on Redis)
- **Not a drop-in replacement for Kombu** — this is a completely breaking change from the original

## Key differences from Kombu

| Kombu | kombu-asyncio |
|-------|---------------|
| `with Connection() as conn:` | `async with Connection() as conn:` |
| `conn.drain_events()` | `await conn.drain_events()` |
| `producer.publish(...)` | `await producer.publish(...)` |
| `message.ack()` | `await message.ack()` |
| Sync + async API | Async only |

## Requirements

- Python 3.14+
- redis-py 7.1+ (Redis transport)
- aio-pika 9+ (AMQP transport, optional)

## License

BSD-3-Clause (same as Kombu)
