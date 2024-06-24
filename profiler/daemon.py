import psutil
import time
import gpustat
import json
import sys
from cs230_common.messenger import PikaMessenger
from cs230_common.utils import MessageCategory, MessageBuilder

if len(sys.argv) == 1:
    raise Exception("Worker ID should be defined")


def profiler_main(config, messenger):
    while True:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_usage = psutil.virtual_memory()
        memory_total = memory_usage.total
        memory_used = memory_usage.total - memory_usage.available
        gpu_usage = 0

        if (
            sys.platform.startswith("win")
            and config["workers"][sys.argv[1]]["GPU"] != 0
        ):
            gpu_stats = gpustat.GPUStatCollection.new_query()
            for gpu in gpu_stats.gpus:
                gpu_usage = gpu.memory_used * 1024 * 1024
        elif not sys.platform.startswith("win"):
            gpu_usage = memory_used

        msg_body = {
            "worker_id": int(sys.argv[1]),
            "cpu_usage": cpu_percent,
            "memory_total": memory_total,
            "memory_used": memory_used,
            "gpu_usage": gpu_usage,
        }

        print(msg_body)
        messenger.produce(
            MessageBuilder.build(MessageCategory.profile, msg_body), "worker_scheduler"
        )
        time.sleep(config["profiler_report_period"])


with open("../worker/config.json", "r") as f:
    config = json.load(f)

messenger = PikaMessenger(
    broker_host=config["broker"]["broker_host"],
    broker_port=config["broker"]["broker_port"],
    receive_topics=[],
)

try:
    messenger.consume()
    profiler_main(config, messenger)
except KeyboardInterrupt:
    messenger.stop_consuming()
finally:
    del messenger
