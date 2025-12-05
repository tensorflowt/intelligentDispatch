from fastapi import FastAPI
import uvicorn
from args import parse_args

from Api.api import APIServer
from utils.logger import setup_logger
from nexuts import InformationCenter
from utils.utils import load_config


def main():
    args = parse_args()
    logger = setup_logger(
        level=args.log_level,
        log_dir=args.log_dir,
        rotation=args.log_file_size,
        retention=args.log_file_retention,
        filename="Nexuts",
    )

    logger.info("==== Nexuts Service Starting ====")
    args = parse_args()
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
      --log-dir /data/Nexuts/logs \
      --log-file-size "200 MB" \
      --log-file-retention "30 days" \
      --log-level INFO \
      --
      
      
python3 main.py --host 0.0.0.0 --port 9991

    """