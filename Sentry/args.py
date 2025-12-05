import argparse


def parse_args():
    parser = argparse.ArgumentParser(description="Sentry Inference Service")

    # 基础运行参数
    parser.add_argument("--host", type=str, default="0.0.0.0", help="Server host")
    parser.add_argument("--port", type=int, default=8000, help="Server port")

    # 运行时配置
    parser.add_argument("--reorder-timeout", type=float, default=5.0, help="Reorder timeout")
    parser.add_argument("--health-interval", type=float, default=10.0, help="Health check interval")
    parser.add_argument("--config_path", type=str, help="Config path", default="./utils/SentryConfig.json")

    # 日志相关配置
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level")
    parser.add_argument("--log-dir", type=str, default="./logs", help="Log directory")
    parser.add_argument("--log-file-size", type=str, default="200 MB", help="Single log file max size")
    parser.add_argument("--log-file-retention", type=str, default="30 days", help="How long to keep old logs")

    return parser.parse_args()
