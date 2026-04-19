import logging


def configure_logging(level: str = "INFO") -> None:
    # keep logs readable and useful for local runs or scheduled execution
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )