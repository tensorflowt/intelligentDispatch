import json
import os


def load_config(file_path: str) -> dict:
    """
    从 JSON 配置文件读取内容并返回为字典。

    :param file_path: JSON 文件路径
    :return: dict 配置内容
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"配置文件不存在: {file_path}")

    with open(file_path, "r", encoding="utf-8") as f:
        try:
            data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"JSON 文件格式错误: {e}")

    if not isinstance(data, dict):
        raise ValueError("配置文件的顶层结构必须是一个对象（dict）")

    return data


# 示例用法
if __name__ == "__main__":
    config = load_config("config.json")
    print(config)