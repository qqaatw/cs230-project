import time
import logging

import json
import os
from ftplib import FTP
from queue import Queue
from functools import partial

from cs230_common.messenger import PikaMessenger

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

PIKA_HOST = "18.223.152.106"
PIKA_PORT = "5673"

# 1. Consume message from API, Fetch file from FTP server, Create task_id & Add file to the new_tasks


class FTPScheduler:
    def __init__(self, host, port, username, password, broker_host=PIKA_HOST, broker_port=PIKA_PORT, receive_topics=["api_to_scheduler"]):
        self.ftp = FTP()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.messenger = PikaMessenger(broker_host, broker_port, receive_topics, self.on_new_file_callback)

        # New tasks fetched from FTP server (hash table)
        self.new_tasks = {}
        self.task_id = 0
        
    def connect_to_ftp(self):
        self.ftp.connect(self.host, self.port)
        self.ftp.login(self.username, self.password)
        # file = open("C:/Users/Kun Woo Park/workspace/cs230/aaa.txt", "rb")
        # self.ftp.storbinary('STOR qqaatw/aaa.txt', file)

    def create_directory_for_task(self, task_id):
        directory_name = str(task_id)
        self.ftp.mkd(directory_name)
        return directory_name
    
    # Consume message from API & call 'fetch_file'
    def on_new_file_callback(self, channel, method, body):
        message = json.loads(body)

        # When API requested new task to scheduler (API message should contain "request", "filename": f"{file_name}")
        if 'request' in message:
            # Create task_id & directory, Add file to the new_tasks
            self.task_id += 1
            directory_name = self.create_directory_for_task(self.task_id)
            self.new_tasks[self.task_id] = message['filename']
            directory_name_message = json.dumps({"task_id": self.task_id, "directory_name": directory_name})
            # Send directory_name to API
            self.messenger.produce(directory_name_message)
            LOGGER.info(f"Directory {directory_name} created for task ID {self.task_id}. Message sent to API.")

        # When API uploaded file to FTP server (API message should contain "uploaded", "task_id": f"{task_id}")
        elif 'uploaded' in message:
            task_id = message['task_id']
            if task_id in self.new_tasks:
                filename = self.new_tasks[task_id]
                directory_name = str(task_id)
                # Send file location to worker server
                file_location_message = json.dumps({"task_id": task_id, "file_location": f"{directory_name}/{filename}"})
                self.messenger.produce(file_location_message)
                LOGGER.info(f"File {filename} in directory {directory_name} uploaded to FTP server. Message sent to worker.")

    def run(self):
        self.connect_to_ftp()
        self.messenger.consume()
    
    def cleanup(self):
        self.ftp.quit()      # TODO : Add what is needed after disconnecting FTP server




# 2. Find available worker, If available put task into ongoing_tasks & if not into waiting_tasks queue, Send "start" to worker & Receive "complete" from worker, Delete task from ongoing_tasks when complete.


# Find available worker
def find_available_worker_id(ongoing_tasks):
    for worker_id in range(1, 4):
        if worker_id not in [task['worker_id'] for task in ongoing_tasks.values()]:
            return worker_id
    return None


# If worker is available put task into ongoing_tasks & if not into waiting_tasks queue
def scheduler_main(ftp_scheduler):
    tasks = ftp_scheduler.new_tasks # task_id, filename
    ongoing_tasks = {}
    waiting_tasks_queue = Queue()    
    worker_report_queue = Queue()
    callback_with_args = partial(worker_message_handler, 
                                 worker_report_queue=worker_report_queue, 
                                 ongoing_tasks=ongoing_tasks, 
                                 waiting_tasks_queue=waiting_tasks_queue)
    messenger = PikaMessenger(broker_host=PIKA_HOST, broker_port=PIKA_PORT, receive_topics=TOPICs, 
                              callback=lambda channel, method, 
                              body: callback_with_args)  
    TOPICs = ["scheduler_to_worker_1", "scheduler_to_worker_2", "scheduler_to_worker_3"]
    messenger.consume()
                      
    for task_id,filename in tasks.items():
        available_worker_id = find_available_worker_id(ongoing_tasks)
                
        # If available put task into ongoing_tasks & send message to the available worker
        if available_worker_id:
            ongoing_tasks[task_id] = {"filename": filename, "worker_id": available_worker_id}
            json_string = json.dumps({"task_id": task_id, "worker_id": available_worker_id, "status": "start"})
            messenger.produce(json_string)

        # If not put task into waiting_tasks queue
        else:
            waiting_tasks_queue.put((task_id, filename))


# Receive "complete" from worker, Delete task from ongoing_tasks & Put waiting task into ongoing_tasks
def worker_message_handler(channel, method, body, worker_report_queue, ongoing_tasks, waiting_tasks_queue):
    callback_with_args = partial(worker_message_handler, 
                                 worker_report_queue=worker_report_queue, 
                                 ongoing_tasks=ongoing_tasks, 
                                 waiting_tasks_queue=waiting_tasks_queue)
    messenger = PikaMessenger(broker_host=PIKA_HOST, broker_port=PIKA_PORT, receive_topics=TOPICs, 
                              callback=lambda channel, method, 
                              body: callback_with_args)
    TOPICs = ["scheduler_to_worker_1", "scheduler_to_worker_2", "scheduler_to_worker_3"]
    
    LOGGER.info(f"Message received: {body}")
    message = json.loads(body)
    task_id = message['task_id']
    status = message['status']

    # Receive status message from woker & put the message into worker_report_queue
    worker_report_queue.put(message)

    # If task is complete, delete from ongoing_tasks
    if status == "complete":
        if task_id in ongoing_tasks:
            del ongoing_tasks[task_id]
            LOGGER.info(f"Task {task_id} completed and removed from the ongoing tasks.")

            # If waiting task exists, find available worker & put the task into ongoing_tasks & send message to the available worker
            if not waiting_tasks_queue.empty():
                next_task_id, filename = waiting_tasks_queue.get()
                available_worker_id = find_available_worker_id(ongoing_tasks)
                if available_worker_id is not None:
                    ongoing_tasks[next_task_id] = {"filename": filename, "worker_id": available_worker_id}
                    json_string = json.dumps({"task_id": next_task_id, "worker_id": available_worker_id, "status": "start"})
                    messenger.produce(json_string)
                else:
                    LOGGER.error("No available worker ID found for the next task.")


if __name__ == "__main__":
    ftp_scheduler = FTPScheduler(host="169.234.3.12", port=21, username="kunwp1", password="test")
    ftp_scheduler.run()
    scheduler_main(ftp_scheduler)