from loguru import logger
import os
import sys
from datetime import datetime


def setup_logger(
    level: str = "INFO",
    log_dir: str = "./logs",
    rotation: str = "100 MB",
    retention: str = "7 days",
    filename: str = "nexuts"
):
    """
    初始化 Loguru 日志系统
    """
    os.makedirs(log_dir, exist_ok=True)
    logger.remove()

    log_format_console = (
        "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
        "<level>{level: <8}</level> | "
        "<cyan>[{name}:{function}:{line}]</cyan> | "
        "<level>{message}</level>"
    )

    log_format_file = (
        "{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
        "[{name}:{function}:{line}] | {message}"
    )

    # 控制台输出
    logger.add(
        sys.stdout,
        level=level,
        colorize=True,
        enqueue=True,
        format=log_format_console
    )

    # 文件输出
    log_file = os.path.join(log_dir, f"{filename}_{datetime.now().strftime('%Y%m%d')}.log")
    logger.add(
        log_file,
        level=level,
        rotation=rotation,
        retention=retention,
        enqueue=True,
        format=log_format_file
    )

    logger.info(f"Logger initialized -> level={level}, dir={log_dir}, rotation={rotation}, retention={retention}")
    return logger

__all__ = ["logger", "setup_logger"]