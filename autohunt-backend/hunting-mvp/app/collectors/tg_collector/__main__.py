from __future__ import annotations

import asyncio
import logging

from .collector import TelethonCollector
from .settings import Settings


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


async def _amain() -> int:
    setup_logging()
    log = logging.getLogger("tg_collector")

    collector: TelethonCollector | None = None
    try:
        s = Settings.load()
        collector = TelethonCollector(s)
        await collector.start()
        return 0

    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutdown requested (Ctrl+C).")
        return 0

    except Exception:
        log.exception("Collector crashed")
        return 1

    finally:
        if collector is not None:
            try:
                await collector.close()
            except Exception:
                log.exception("Error during final close()")


def main() -> None:
    raise SystemExit(asyncio.run(_amain()))


if __name__ == "__main__":
    main()
