# src/utils/logger.py
import logging
import sys

def get_logger(name: str) -> logging.Logger:
    """
    Retorna um logger configurado com formato padrão do projeto.
    Uso: logger = get_logger(__name__)
    """
    logger = logging.getLogger(name)

    if not logger.handlers:  # evita duplicar handlers se importado várias vezes
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger