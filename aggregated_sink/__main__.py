"""Entry point: python -m aggregated_sink

Streams the radiation.aggregated Kafka topic into Postgres (ADR-018). All config
is environment-driven — see aggregated_sink/config.py and .env.example.
"""

from __future__ import annotations

import logging

from .config import Config
from .consumer import run


def main() -> None:
    config = Config.from_env()
    logging.basicConfig(
        level=config.log_level,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    run(config)


if __name__ == "__main__":
    main()
