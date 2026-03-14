# kombu-asyncio

Asyncio-native rewrite of [Kombu](https://github.com/celery/kombu). This is the messaging layer for [celery-asyncio](https://github.com/oliverhaas/celery-asyncio).

This package exists as a standalone library for compatibility with packages that import from `kombu` directly (e.g. Celery Flower). Long-term it will be merged into celery-asyncio.

## Requirements

- Python 3.14+
- redis-py 7.1+ (Redis transport)
- aio-pika 9+ (AMQP transport, optional)

## License

BSD-3-Clause (same as Kombu)
