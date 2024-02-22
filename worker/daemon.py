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
        tempdir = tempfile.TemporaryDirectory("")
        
        command = f"conda env create --prefix {tempdir.name} -f {self.path}".split(" ")
        print(command)
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=SHELL) # use Popen instead of run
        try:
            stdoutdata, stderrdata = proc.communicate(timeout=5) # use communicate with timeout
            print(stdoutdata.decode('utf-8'), stderrdata.decode('utf-8'))
            return tempdir.name
        except subprocess.TimeoutExpired:
            print("The command timed out")
            proc.kill() # kill the subprocess if timeout
            tempdir.cleanup()
            return "cs230" # return pre-created
        except KeyboardInterrupt: # catch keyboard interrupt
            print("The user interrupted the command")
            proc.terminate() # terminate the subprocess if interrupt
            return "cs230" # return empty string if interrupt


"""
Worker thread receives message in a json format from scheduler, parse the json string, and execute the worker_task
"""

def main():
    with open("config.json", "r") as f:
        config = json.load(f)

    messenger = PikaMessenger(
        broker_host=config["broker"]["broker_host"],
        broker_port=config["broker"]["broker_port"],
        receive_topics=[]
    )

    try:    
        messenger.consume()
        worker_main(config, messenger)
    except KeyboardInterrupt:
        messenger.stop_consuming()
    finally:
        del messenger
        
def worker_main(config, messenger):
    process = {}
    while True:
        handle_completed_process(process, messenger, config)

        if messenger.q.empty():
            time.sleep(1)
            continue

        _, json_string = messenger.q.get()
        
        data = json.loads(json_string)
        if data["CATEGORY"] == MessageCategory.scheduled_task:
            task_id = data["body"]["task_id"]
            p, env = run_task(data["body"], config)
            process[(task_id, env)] = p
        elif data["CATEGORY"] == MessageCategory.queue_request_response:
            continue
        else:
            raise RuntimeError(
                f"This is not the message expected from the scheduler.")


def run_task(body, config):
    task_id = body["task_id"]
    FTPServer = FileTransferClient(
        host=config["broker"]["broker_host"], port=int(config["ftp"]["ftp_port"]), username="kunwp1", password="test"
    )
    FTPServer.fetch_file(task_id)
    conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    tempdir_name = conda_manager.build()
    os.chdir(str(task_id))
    conda_command = (
        "conda run -p {} ".format(os.path.join(os.path.dirname(__file__).replace("\\\\", "\\"), tempdir_name))
        + "TASK_ID=" + str(task_id) + " " + body["python_command"]
    )
    print(conda_command)
    p = subprocess.Popen(conda_command, shell=True, stdout=open(str(task_id) + "stdout.txt", "w"), stderr=open(str(task_id) + "stderr.txt", "w"))
    os.chdir('..')
    return p, tempdir_name

def handle_completed_process(process, messenger, config):
    to_remove = []
    for key in process:
        task_id = key[0]
        env = key[1]
        exit_code = process[key].poll()
        if exit_code is not None:
            process[key].terminate()
            if env != "cs230":
                shutil.rmtree(env)
            #shutil.rmtree(str(task_id))
            msg_body = {"task_id": task_id, "status": exit_code}
            messenger.produce(
                MessageBuilder.build(MessageCategory.task_status, msg_body), "worker_scheduler"
            )
            to_remove.append(key)
            
    for key in to_remove:
        del process[key]

def test():
    with open("config.json", "r") as f:
        config = json.load(f)
    conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    conda_manager.build()


if __name__ == "__main__":
    main()
