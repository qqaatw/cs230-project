from cs230_common.messenger import PikaMessenger
from cs230_common.file_transfer_client import FileTransferClient
from cs230_common.utils import MessageCategory, MessageBuilder
import sys
import json
import subprocess
import logging
import os
import threading
import time
import tempfile

logging.basicConfig(level=logging.DEBUG)

SHELL = False
if sys.platform.startswith("win"):
    SHELL = True


class CondaManager:
    def __init__(self, name: str, path: str):
        self.name = name
        self.path = path

    def build(self):
        tempdir = tempfile.TemporaryDirectory()
        tempdir_name = os.path.basename(tempdir.name)
        if os.path.exists(tempdir_name):
            raise RuntimeError("Directory {tempdir_name} already exists.")

        mkdir = f"mkdir {tempdir_name}".split()
        print(mkdir)
        command = f"conda env create --prefix {tempdir_name} -f {self.path}".split(" ")
        print(command)
        result = subprocess.run(command, capture_output=True, text=True, shell=SHELL)
        print(result.stdout, result.stderr)
        return tempdir_name


"""
Worker thread receives message in a json format from scheduler, parse the json string, and execute the worker_task
"""


def worker_main():
    with open("config.json", "r") as f:
        config = json.load(f)

    messenger = PikaMessenger(
        broker_host=config["broker"]["broker_host"],
        broker_port=config["broker"]["broker_port"],
        topics=["worker_to_scheduler"],
    )

    # create a thread to receive messages and put them in the queue
    messenger.comsume()

    processes = {}

    while True:
        for tid, p in processes:
            exit_code = p.poll()
            if exit_code is not None:
                p.terminate()
                msg_body = json.dumps({"task_id": tid, "status": exit_code})
                messenger.produce(
                    MessageBuilder.build(MessageCategory.task_status, msg_body)
                )

        if messenger.q.empty():
            time.sleep(1)
            continue

        json_string = messenger.q.get()
        data = json.loads(json_string)
        if data["CATEGORY"] == MessageCategory.scheduled_task:
            task_id = data["task_id"]
            p = run_task(data["body"], config)
            processes[task_id] = p
        else:
            raise RuntimeError(
                f"This is not the message expected from the worker: {0}"
            ).format(data["command"])


def run_task(msg_body, config):
    data = json.loads(msg_body)
    task_id = data["task_id"]
    FTPServer = FileTransferClient(
        host="192.168.0.186", port=21, username="kunwp1", password="test"
    )
    FTPServer.fetch_file(task_id)

    conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    tempdir_name = conda_manager.build()

    os.chdir(task_id)
    conda_command = (
        f"conda run -n {os.path.join(os.path.dirname(__file__), tempdir_name)} --prefix "
        + data["python_command"]
    )
    p = subprocess.Popen(conda_command, shell=True)
    return p


def test():
    with open("config.json", "r") as f:
        config = json.load(f)
    conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    conda_manager.build()


if __name__ == "__main__":
    # test()
    print(os.path.join(os.path.dirname(__file__), "tmp2x2ureu2"))
