import requests  
import time  
  
def test_prompt_push():  
    # Sentry API地址  
    sentry_url = "http://127.0.0.1:9992/v1/radixtree/update"  
      
    # 测试数据 - 插入提示词节点  
    test_data = {  
        "ops_id": 1,  
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00"),  
        "node_ip": "172.16.16.63",  
        "server_port": 9993,  
        "instance_id": "test_prefill_zwt",  
        "info": [{  
            "op_type": "insert_node",  
            "parent_path": [],  
            "prompt": [0, 1, 2, 3],  
            "prompt_value": [1, 2, 3, 4],  
            "insert_key": [0, 1, 2, 3],  
            "insert_value": [1, 2, 3, 4],  
            "split_length": 0,  
            "depth": 1  
        }]  
    }  
      
    # 发送请求  
    response = requests.post(sentry_url, json=test_data)  
    print(f"Response: {response.json()}")  
      
    # 测试删除节点  
    delete_data = {  
        "ops_id": 2,  
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S.%f+00:00"),  
        "node_ip": "172.16.16.63",  
        "server_port": 9993,  
        "instance_id": "test_prefill_zwt",  
        "info": [{  
            "op_type": "delete_node",  
            "parent_path": [],  
            "prompt": [0, 1, 2, 3],  
            "split_length": 4,  
            "depth": 1  
        }]  
    }  
      
    response = requests.post(sentry_url, json=delete_data)  
    print(f"Delete Response: {response.json()}")  
  
if __name__ == "__main__":  
    test_prompt_push()
