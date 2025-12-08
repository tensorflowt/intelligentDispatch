# 1.启动中心
cd intelligentDispatch/Nexuts
python3 main.py --host 0.0.0.0 --port 8000

# 2.启动Sentry节点（2p1d需要2个节点）
## Sentry节点1  
cd Sentry  
python3 start_sentry.py --host 0.0.0.0 --port 9993  
## Sentry节点2    
cd Sentry  
python3 start_sentry.py --host 0.0.0.0 --port 9994

# 3.启动2p1d mock服务并注册对应Sentry
python mock_service.py --port 9000 --type prefill --sentry-port 9993
python mock_service.py --port 9001 --type prefill --sentry-port 9993
python mock_service.py --port 9002 --type decode --sentry-port 9994

# 4.插入缓存数据
curl -X POST http://127.0.0.1:9993/v1/radixtree/update \
  -H "Content-Type: application/json" \
  -d '{    
   "ops_id": 1,    
    "timestamp": "2025-12-08T10:00:00",    
    "node_ip": "127.0.0.1",    
    "server_port": 9000,    
    "instance_id": "mock_prefill_9000",    
    "info": [{    
        "op_type": "insert_node",    
        "parent_path": [],    
        "prompt": [100, 200, 300, 400],    
        "prompt_value": [1, 2, 3, 4],    
        "insert_key": [100, 200, 300, 400],    
        "insert_value": [1, 2, 3, 4],    
        "split_length": 0,    
        "depth": 1 }]  
  }'
  
curl -X POST http://127.0.0.1:9993/v1/radixtree/update \
  -H "Content-Type: application/json" \
  -d '{    
   "ops_id": 2,  
    "timestamp": "2025-12-08T10:00:01",   
    "node_ip": "127.0.0.1",  
    "server_port": 9001,  
    "instance_id": "mock_prefill_9001",  
    "info": [{  
        "op_type": "insert_node",  
        "parent_path": [],  
        "prompt": [500, 600, 700],  
        "prompt_value": [5, 6, 7],  
        "insert_key": [500, 600, 700],  
        "insert_value": [5, 6, 7],  
        "split_length": 0,  
        "depth": 1 }]  
  }'
  
# 5.测试 
curl -X GET "http://127.0.0.1:9991/v1/Nexuts/get_best_instance?prompt_tokens=100,200,300,400,500" 