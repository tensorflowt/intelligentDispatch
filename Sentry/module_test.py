import argparse
import requests
import json

BASE_URL = "http://127.0.0.1:8000"   # 改成你的 APIServer 地址


def test_register():
    url = f"{BASE_URL}/v1/Sentry/register_inference_info"
    payload = {
        "instance_type": "decode",
        "instance_id": "inst_123",
        "node_ip": "127.0.0.1",
        "service_port": 8001,
        "tp_size": 1,
        "base_gpu_id": 0,
        "step": 1
    }

    r = requests.post(url, json=payload)
    print("POST", url)
    print("Status:", r.status_code)
    print("Response:", r.text)


def test_update():
    url = f"{BASE_URL}/v1/radixtree/update"
    payload = {
        "ops_id": 1,
        "timestamp": "2025-01-01T00:00:00",
        "node_ip": "127.0.0.1",
        "server_port": 8002,
        "instance_id": "inst_123",
        "info": [
            {
                "op_type": "insert",
                "parent_path": [[101, 102]],
                "prompt": [101, 102, 103],
                "insert_key": [103],
                "insert_value": [9001],
                "split_length": 0,
                "depth": 2
            }
        ]
    }

    r = requests.post(url, json=payload)
    print("POST", url)
    print("Status:", r.status_code)
    print("Response:", r.text)


def test_health():
    url = f"{BASE_URL}/v1/health"
    r = requests.get(url)
    print("GET", url)
    print("Status:", r.status_code)
    print("Response:", r.text)


def test_instances():
    url = f"{BASE_URL}/v1/instances"
    r = requests.get(url)
    print("GET", url)
    print("Status:", r.status_code)
    print("Response:", r.text)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test",
        type=str,
        required=True,
        choices=["register", "update", "health", "instances"],
        help="选择要执行的测试接口"
    )

    args = parser.parse_args()

    if args.test == "register":
        test_register()
    elif args.test == "update":
        test_update()
    elif args.test == "health":
        test_health()
    elif args.test == "instances":
        test_instances()