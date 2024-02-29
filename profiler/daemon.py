import psutil
import time
import gpustat
import json
import sys
import torch
from cs230_common.messenger import PikaMessenger
from cs230_common.utils import MessageCategory, MessageBuilder

def profiler_main(config, messenger):
    while True:
        cpu_percent = psutil.cpu_percent(interval=1)
        #print(f"CPU Usage: {cpu_percent}%")
        memory_usage = psutil.virtual_memory()
        #print(memory_usage.total)
        #print(f"Memory Usage: {memory_usage.percent}%")
        disk_usage = psutil.disk_usage('/')
        #print(f"Disk Usage: {disk_usage.percent}%")
        gpu_usage = 0
        
        if sys.platform.startswith("win"):
            gpu_stats = gpustat.GPUStatCollection.new_query()
            for gpu in gpu_stats.gpus:
                #print(f"GPU {gpu.index}: {gpu.name}, Utilization: {gpu.memory_used}")
                gpu_usage = gpu.memory_used * 1024 * 1024
        else:
            gpu_usage = torch.mps.current_allocated_memory()
            
        msg_body = {"cpu_usage": cpu_percent, "memory_usage": memory_usage.percent, "disk_usage": disk_usage.percent,
                    "gpu_usage": gpu_usage}
        messenger.produce(
            MessageBuilder.build(MessageCategory.profile, msg_body), "worker_scheduler"
        )
        time.sleep(config["profiler_report_period"])

with open("../worker/config.json", "r") as f:
    config = json.load(f)

messenger = PikaMessenger(
    broker_host=config["broker"]["broker_host"],
    broker_port=config["broker"]["broker_port"],
    receive_topics=[]
)

try:    
    messenger.consume()
    profiler_main(config, messenger)
except KeyboardInterrupt:
    messenger.stop_consuming()
finally:
    del messenger