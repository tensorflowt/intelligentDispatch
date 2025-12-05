import requests  
import time  
import subprocess  
import os  
  
def clear_test_data():  
    """清理测试数据"""  
    db_path = "/data/info_center.db"  
    if os.path.exists(db_path):  
        os.remove(db_path)  
        print(f"Removed {db_path}")  
  
def start_mock_sentry():  
    """启动模拟Sentry服务"""  
    cmd = ["python3", "mock_sentry.py"]  
    proc = subprocess.Popen(cmd)  
    time.sleep(3)  
    return proc  
  
def start_mock_instances():  
    """启动多个模拟实例"""  
    ports = [9000, 9001, 9002]  
    processes = []  
      
    for port in ports:  
        cmd = ["python3", "mock_instance.py", str(port)]  
        proc = subprocess.Popen(cmd)  
        processes.append(proc)  
        time.sleep(1)  
          
    return processes  
  
def stop_instances(processes):  
    """停止所有进程"""  
    for proc in processes:  
        proc.terminate()  
        proc.wait()  
  
def wait_for_services_ready():  
    """等待服务就绪"""  
    nexuts_url = "http://127.0.0.1:8080"  
    sentry_url = "http://127.0.0.1:9992"  
      
    # 等待Nexuts就绪  
    for _ in range(10):  
        try:  
            response = requests.get(f"{nexuts_url}/v1/Nexuts/health", timeout=1)  
            if response.status_code == 200:  
                print("Nexuts is ready")  
                break  
        except:  
            time.sleep(1)  
      
    # 等待Sentry就绪  
    for _ in range(10):  
        try:  
            response = requests.get(f"{sentry_url}/v1/health", timeout=1)  
            if response.status_code == 200:  
                print("Sentry is ready")  
                break  
        except:  
            time.sleep(1)  
  
def test_load_balancing():  
    # 清理旧数据  
    #clear_test_data()  
      
    # 启动Sentry和模拟实例  
    sentry_proc = start_mock_sentry()  
    instance_procs = start_mock_instances()  
      
    # 等待Nexuts服务就绪（假设已在外部启动）  
    wait_for_services_ready()  
      
    all_procs = [sentry_proc] + instance_procs  
      
    try:  
        nexuts_url = "http://127.0.0.1:8080"  
          
        # 注册实例  
        instances = [  
            {"instance_id": "prefill_001", "service_port": 9000},  
            {"instance_id": "prefill_002", "service_port": 9001},  
            {"instance_id": "prefill_003", "service_port": 9002}  
        ]  
          
        for inst in instances:  
            response = requests.post(  
                f"{nexuts_url}/v1/Nexuts/register",  
                json={  
                    "instance_type": "prefill",  
                    "sentry_id": "test_sentry",  
                    **inst,  
                    "node_ip": "127.0.0.1",  
                    "sentry_port": 9992  
                }  
            )  
            print(f"Register {inst['instance_id']}: {response.json()}")  
          
        time.sleep(2)  
          
        # 测试负载均衡  
        for i in range(2):  
            response = requests.get(f"{nexuts_url}/v1/Nexuts/get_best_instance")  
            result = response.json()  
            print(f"\nRound {i}:")  
            print(f"  Selected instance: {result['instance_id']}")  
              
            if 'load_info' in result:  
                print(f"  Load info: {result['load_info']}")  
            else:  
                print(f"  Message: {result.get('message', 'No load info available')}")  
              
        time.sleep(2)  
              
    finally:  
        stop_instances(all_procs)  
  
if __name__ == "__main__":  
    test_load_balancing()