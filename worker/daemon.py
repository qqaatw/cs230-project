from cs230_common.messenger import PikaMessenger
from cs230_common.file_transfer_client import FileTransferClient
from cs230_common.utils import MessageCategory, MessageBuilder
import sys
import json
import subprocess
import logging
import os
import time
import tempfile
import shutil

logging.basicConfig(level=logging.INFO)

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


def main():
    with open("config.json", "r") as f:
        config = json.load(f)

    messenger = PikaMessenger(
        broker_host=config["broker"]["broker_host"],
        broker_port=config["broker"]["broker_port"],
        receive_topics=["worker_to_scheduler"],
    )

    # create a thread to receive messages and put them in the queue
    messenger.consume()

    try:
        worker_main(config, messenger)
    except KeyboardInterrupt:
        messenger.stop_consuming()
    finally:
        del messenger
        
def worker_main(config, messenger):
    process = {}
    while True:
        for key in process:
            task_id = key[0]
            env = key[1]
            exit_code = process[key].poll()
            if exit_code is not None:
                process[key].terminate()
                shutil.rmtree(env)
                shutil.rmtree(str(task_id))
                msg_body = {"task_id": task_id, "status": exit_code}
                messenger.produce(
                    MessageBuilder.build(MessageCategory.task_status, msg_body), "worker_to_scheduler"
                )

        if messenger.q.empty():
            time.sleep(1)
            continue

        _, json_string = messenger.q.get()
        data = json.loads(json_string)
        if data["CATEGORY"] == MessageCategory.scheduled_task:
            task_id = data["body"]["task_id"]
            p, env = run_task(data["body"], config)
            process[(task_id, env)] = p
        else:
            raise RuntimeError(
                f"This is not the message expected from the worker.")


def run_task(body, config):
    task_id = body["task_id"]
    FTPServer = FileTransferClient(
        host="18.223.152.106", port=21, username="kunwp1", password="test"
    )
    FTPServer.fetch_file(task_id)
    conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    tempdir_name = conda_manager.build()
    
    os.chdir(str(task_id))
    conda_command = (
        "conda run -p {} ".format(os.path.join(os.path.dirname(__file__).replace("\\\\", "\\"), tempdir_name))
        + body["python_command"]
    )
    p = subprocess.Popen(conda_command, shell=True, stdout=subprocess.PIPE)
    os.chdir('..')
    return p, tempdir_name


def test():
    with open("config.json", "r") as f:
        config = json.load(f)
    conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    conda_manager.build()


if __name__ == "__main__":
    main()
