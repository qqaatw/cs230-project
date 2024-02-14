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

HOST = "18.223.214.228"
PORT = "5673"

# 1. Consume message from API, Fetch file from FTP server, Create task_id & Add file to the new_tasks


class FTPScheduler:
    def __init__(self, host, port, username, password, remote_dir, receive_topics, broker_host=HOST, broker_port=PORT):
        self.ftp = FTP()
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.remote_dir = remote_dir
        self.receive_topics = ["api_to_ftp"]
        self.messenger = PikaMessenger(broker_host, broker_port, receive_topics, self.on_new_file_callback)

        # New tasks fetched from FTP server (hash table)
        self.new_tasks = {}
        self.task_id = 0
        
    def connect_to_ftp(self):
        self.ftp.connect(self.host, self.port)
        self.ftp.login(self.username, self.password)
        self.ftp.cwd(self.remote_dir)
        
    # Fetch file from FTP server
    def fetch_file(self, filename):
        local_filename = os.path.join('temp', filename)
        with open(local_filename, 'wb') as file:
            self.ftp.retrbinary('RETR ' + filename, file.write)

        # Create task_id & Add file to the new_tasks
        self.task_id += 1      # Scheduler creates task_id
        self.new_tasks[self.task_id] = filename
        print(f"Task {filename} fetched from FTP server. Task ID: {self.task_id}")

    # Consume message from API & call 'fetch_file'
    def on_new_file_callback(self, channel, method, body):
        message = json.loads(body)
        if 'filename' in message:
            self.fetch_file(message['filename'])
        
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
    messenger = PikaMessenger(broker_host=HOST, broker_port=PORT, receive_topics=TOPICs, 
                              callback=lambda channel, method, 
                              body: callback_with_args)  
    TOPICs = ("scheduler_to_worker_1", "scheduler_to_worker_2", "scheduler_to_worker_3")
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
    messenger = PikaMessenger(broker_host=HOST, broker_port=PORT, receive_topics=TOPICs, 
                              callback=lambda channel, method, 
                              body: callback_with_args)
    TOPICs = "scheduler_to_worker_1", "scheduler_to_worker_2", "scheduler_to_worker_3"
    
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
    ftp_scheduler = FTPScheduler(host="", port="", username="username", password="password", remote_dir="remote_dir", broker_host="", broker_port="", receive_topics=["receive_topic"])
    ftp_scheduler.run()
    scheduler_main(ftp_scheduler)