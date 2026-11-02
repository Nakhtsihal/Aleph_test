import asyncio
import logging
from enricher import Enricher
from logging_config import setup_logging

logger = logging.getLogger(__name__)


async def main() -> None:
    enricher = Enricher()
    try:
        await enricher.load_enrichment("data/enrichment.json")
        logger.info(
            "Enrichment data loaded",
            extra={"action": "load_enrichment", "status": "success"},
        )
    finally:
        await enricher.close_connection()

if __name__ == "__main__":
    setup_logging()
    try:
        asyncio.run(main())
    except Exception:
        logger.exception(
            "Enrichment loading failed",
            extra={"action": "load_enrichment", "status": "error"},
        )
        raise SystemExit(1)
