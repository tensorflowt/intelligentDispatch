from fastapi import FastAPI  
import uvicorn  
import random  
  
app = FastAPI()  
  
@app.get("/metrics")  
def metrics():  
    """模拟Prometheus metrics，返回随机负载"""  
    prealloc = random.randint(0, 10)  
    infight = random.randint(0, 5)  
      
    return f"""# HELP sglang:num_prefill_prealloc_queue_reqs The number of requests in the prefill prealloc queue.  
# TYPE sglang:num_prefill_prealloc_queue_reqs gauge  
sglang:num_prefill_prealloc_queue_reqs{{engine_type="unified",model_name="/work/yansiyu/models/Qwen3-4B",pp_rank="0",tp_rank="0"}} {prealloc}.0  
# HELP sglang:num_prefill_infight_queue_reqs The number of requests in the prefill infight queue.  
# TYPE sglang:num_prefill_infight_queue_reqs gauge  
sglang:num_prefill_infight_queue_reqs{{engine_type="unified",model_name="/work/yansiyu/models/Qwen3-4B",pp_rank="0",tp_rank="0"}} {infight}.0  
"""  
  
@app.get("/v1/model/health")  
def health():  
    return {"status": "ok"}  
  
if __name__ == "__main__":  
    import sys  
    port = int(sys.argv[1])  
    uvicorn.run(app, host="0.0.0.0", port=port)