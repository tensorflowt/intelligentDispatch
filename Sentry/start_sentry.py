import os
import uvicorn

from args import parse_args
from utils.logger import setup_logger
from ApiServer.api_server import APIServer


def main():
    # 1. 解析命令行参数
    args = parse_args()

    # 2. 初始化日志系统（loguru）
    logger = setup_logger(
        level=args.log_level,
        log_dir=args.log_dir,
        rotation=args.log_file_size,
        retention=args.log_file_retention,
        filename="sentry",
    )
    logger.info("==== Sentry Service Starting ====")

    # 3. 初始化 API 服务
    server = APIServer(args)

    app = server.get_app()

    # 4. 启动 FastAPI 服务（由 uvicorn 负责）
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()

"""
python main.py \
  --host 0.0.0.0 \
  --port 8080 \
  --log-dir /data/sentry/logs \
  --log-file-size "200 MB" \
  --log-file-retention "30 days" \
  --log-level DEBUG
  
  
  python3 start_sentry.py --host 0.0.0.0 --port 9992

"""