from common.cs230_common.messenger import PikaMessenger
from common.cs230_common.file_transfer_client import FileTransferClient
import sys
import json
import subprocess
import logging
import os
from queue import Queue
import threading
import time

logging.basicConfig(level=logging.DEBUG)

SHELL = False
if sys.platform.startswith("win"):
    SHELL = True


class CondaManager:
    def __init__(self, name: str, path: str):
        self.name = name
        self.path = path


    def build(self):
        if os.path.exists('env'):
            raise RuntimeError("Directory `env` already exists.")
    
        mkdir = f"mkdir env".split()
        print(mkdir)
        result = subprocess.run(mkdir, capture_output=True, text=True, shell=SHELL)
        command = f"conda env create --prefix env -f {self.path}".split(" ")
        print(command)
        result = subprocess.run(command, capture_output=True, text=True, shell=SHELL)
        print(result.stdout, result.stderr)


"""
Worker thread receives message in a json format from scheduler, parse the json string, and execute the worker_task
"""


def worker_main():
    with open("config.json", "r") as f:
        config = json.load(f)

    # conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    # conda_manager.build()

    queue = Queue()
    messenger = PikaMessenger(
        broker_host=config["broker"]["broker_host"],
        broker_port=config["broker"]["broker_port"],
        topics=[v for v in config["broker"]["topics"].values()],
    )

    # create a thread to receive messages and put them in the queue
    receiver_thread = threading.Thread(target=receiver, args=(queue, messenger))
    receiver_thread.start()

    running_tasks = {}

    while True:
        if queue.empty():
            time.sleep(1)
            continue

        json_string = queue.get()
        data = json.loads(json_string)
        if data["CATEGORY"] == "scheduled_task":
            run_task(data["body"])
        else:
            raise RuntimeError(
                f"This is not the message expected from the worker: {0}"
            ).format(data["command"])


def receiver(queue, messenger):
    def callback(message):
        queue.put(message)

    messenger.consume(callback)


def run_task(msg_body):
    data = json.loads(msg_body)
    task_id = data["task_id"]
    FTPServer = FileTransferClient(
        host="192.168.0.186", port=21, username="kunwp1", password="test"
    )
    FTPServer.fetch_file(task_id)
    os.chdir(task_id)
    # TODO: conda manager
    e = threading.Event()
    t = threading.Thread(target=run_python, args=(data["python_command"], e))
    t.start()
    return t, e


def run_python(command, e):
    p = subprocess.Popen(command, shell=True)
    while not e.is_set() and p.poll() is None:
        time.sleep(0.1)
    if e.is_set():
        p.terminate()


def test():
    with open("config.json", "r") as f:
        config = json.load(f)
    conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    conda_manager.build()


if __name__ == "__main__":
    worker_main()
