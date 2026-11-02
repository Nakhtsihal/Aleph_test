import json
import logging


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:

        logger_dict = {
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "action": getattr(record, "action", None),
            "status": getattr(record, "status", None)
        }
        if record.exc_info:
            logger_dict["exception"] = self.formatException(record.exc_info)

        return json.dumps(logger_dict, ensure_ascii=False)


def setup_logging() -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(
        level=logging.INFO,
        handlers=[handler]
    )
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        uvicorn_logger = logging.getLogger(name)
        uvicorn_logger.handlers = [handler]
        uvicorn_logger.propagate = False
