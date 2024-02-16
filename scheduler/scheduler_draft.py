import time
import logging

import json
import os
from ftplib import FTP
from queue import Queue
from functools import partial

from cs230_common.messenger import PikaMessenger
from cs230_common.file_transfer_client import FileTransferClient
from cs230_common.utils import *

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

HOST = "18.223.152.106"
PORT = "5673"
TOPICS = ["api_to_scheduler", "scheduler_to_worker_1", "scheduler_to_worker_2", "scheduler_to_worker_3"]

class Scheduler:
    def __init__(self):
        self.new_tasks = {} # key: task_id, {username, python_command}
        self.ongoing_tasks = {} # key: task_id, {username, python_command, worker_id}
        self.waiting_tasks_queue = Queue()
        self.worker_report_queue = Queue()
        self.messenger = PikaMessenger(broker_host=HOST, broker_port=PORT, receive_topics=TOPICS)
        self.task_id = 0

    def message_consume(self):
        self.messenger.consume(callback=self.on_message_received)

    # Find available worker
    def find_available_worker(self, ongoing_tasks):
        for worker_id in range(1, 4):
            if worker_id not in [task['worker_id'] for task in self.ongoing_tasks.values()]:
                return worker_id
        return None



# 1. If worker is available put task into ongoing_tasks & if not into waiting_tasks queue
    def new_tasks_scheduler(self):
        new_tasks = self.new_tasks # key: task_id, {username, python_command}
                      
        for task_id, username, python_command in new_tasks.items():
            available_worker_id = self.find_available_worker(self.ongoing_tasks)
                
            # If available put task into ongoing_tasks & send message to the available worker
            if available_worker_id:
                self.ongoing_tasks[task_id] = {"username": username, "python_command": python_command, "worker_id": available_worker_id}
                start_new_tasks_body = json.dumps({"task_id": task_id, "username": username, "python_command": python_command, "worker_id": available_worker_id, "action": "start"})
                start_new_tasks_msg = MessageBuilder.build(MessageCategory.scheduled_task, body : start_new_tasks_body, status : str = "OK")
                self.messenger.produce_fanout(start_new_tasks_msg)

            # If not put task into waiting_tasks queue
            else:
                self.waiting_tasks_queue.put((task_id, username, python_command))

        # After scheduler put into ongoing_tasks/waiting_tasks, delete the task from new_tasks
        del self.new_tasks[task_id]

# 1-1. Execute new_tasks_scheduler periodically
    def start_new_tasks_scheduler(self):
        while True:
            self.new_tasks_scheduler()
            time.sleep(1)

# 2. Consume message from API & workers
    def on_message_received(self, channel, method, body):
        message = json.loads(body)

        # When API requested new task to scheduler (API message should contain "request")
        if message['CATEGORY'] == MessageCategory.queue_request:
            self.new_task_requested(message)

        # When API uploaded file to FTP server (API message should contain "uploaded", "task_id": f"{task_id}")
        elif message['CATEGORY'] == MessageCategory.queue_file_uploaded:
            self.new_file_uploaded(message)

        # When worker sent task status to scheduler (worker message should contain "status")
        elif message['CATEGORY'] == MessageCategory.task_status:
            self.worker_message_handler(message)

# 2-1. Consume message (new_task_request) from API, Create task_id & Add file to the new_tasks
    def new_task_requested(self, message):
        self.task_id += 1
        username = message("username")
        python_command = message("python_command")

        self.new_tasks[self.task_id] = {"username": username, "python_command": python_command}

        task_id_body = json.dumps({"Task ID created. task_id": self.task_id})
        task_id_message = MessageBuilder.build(MessageCategory.queue_request_response, body : task_id_body, status : str = "OK")
        # Send task_id to API
        self.messenger.produce_fanout(task_id_message)
        LOGGER.info(f"Task ID {self.task_id} created and sent to API.")

# 2-2. Consume message (new_file_uploaded) from API, Check new_file in FTP server & Send file_location to API
    '''
    def new_file_uploaded(self, message):
        task_id = message['task_id']
        FTPServer = FileTransferClient(host="192.168.0.186", port=21, username="kunwp1", password="test")
        if task_id in self.new_tasks:
            filename = self.new_tasks[task_id]
            # Send file location to worker server
            file_location = FTPServer.ftp.nlst() # TODO
            file_location_message = json.dumps({"task_id": task_id, "file_location": f"{file_location}"})      # TODO: Find file location in FTP server 
            self.messenger.produce(file_location_message, topic=TOPICS[0])
            LOGGER.info(f"Task ID {task_id}, file {filename} uploaded to {file_location}.")
    '''
# 2-3. Consume message (task_status) from workers
# 2-3. If task is completed, Delete from ongoing_tasks & Put waiting task into ongoing_tasks and send start message to worker
    def worker_message_handler(self, message):    
        LOGGER.info(f"Message received: {message}")
        task_id = message['task_id']
        status = message['status']

        # Put status message into worker_report_queue
        self.worker_report_queue.put(message)

        # If task is completed, delete from ongoing_tasks
        if status == "complete":
            if task_id in self.ongoing_tasks:
                del self.ongoing_tasks[task_id]
                LOGGER.info(f"Task ID {task_id} completed and removed from ongoing_tasks.")

                # If waiting task exists, find available worker & put the task into ongoing_tasks & send message to the available worker
                if not self.waiting_tasks_queue.empty():
                    next_task_id, username, python_command = self.waiting_tasks_queue.get()
                    available_worker_id = self.find_available_worker(self.ongoing_tasks)
                    if available_worker_id is not None:
                        self.ongoing_tasks[next_task_id] = {"username": username, "python_command": python_command, "worker_id": available_worker_id}
                        start_next_task_body = json.dumps({"task_id": next_task_id, "username": username, "python_command": python_command, "worker_id": available_worker_id, "action": "start"})
                        start_next_task_msg = MessageBuilder.build(MessageCategory.scheduled_task, body : start_next_tasks_body, status : str = "OK")
                        self.messenger.produce_fanout(start_next_task_msg)
                    else:
                        LOGGER.error("No available worker ID found for the next task.")


def scheduler_main():
    scheduler = Scheduler()
    scheduler.message_consume()
    scheduler.start_new_tasks_scheduler()

if __name__ == "__main__":
    scheduler_main()