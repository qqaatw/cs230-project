import time
import logging

import json
import os
from queue import Queue
import sys

from cs230_common.messenger import PikaMessenger
from cs230_common.file_transfer_client import FileTransferClient
from cs230_common.utils import *
from priority_based import PriorityQueue, Task
from measure_elapsed_time import Elapsed_Time_Tracker

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

HOST = "18.119.97.104"
PORT = "5673"
TOPICS = ["api_to_scheduler", "worker_scheduler"]
SCHEDULING_ALGORITHMS = ["next-available", "round-robin", "priority-based"]
NUMBER_WORKERS = 2;

# Assume the WORKER_MEMORY_SIZE is in descending order
with open("../worker/config.json", "r") as f:
    config = json.load(f)
    WORKER_MEMORY_SIZE = []
    for i in range(NUMBER_WORKERS):
        WORKER_MEMORY_SIZE.append(config["workers"][str(i+1)]["GPU"])

class Scheduler:
    def __init__(self):
        self.new_tasks = {}  # key: task_id, {username, python_command}
        self.ongoing_tasks = {}  # key: task_id, {username, python_command, worker_id}
        self.initialize_waiting_tasks()
        
        self.worker_report_queue = Queue()
        self.messenger = PikaMessenger(
            broker_host=HOST,
            broker_port=PORT,
            receive_topics=TOPICS,
            callback=self.on_message_received,
        )
        self.task_id = 0
        self.last_assigned_worker_id = 0
        self.elapsed_time_tracker = Elapsed_Time_Tracker()

    def initialize_waiting_tasks(self):
        # Next-available
        if sys.argv[1] == SCHEDULING_ALGORITHMS[0]:
            self.waiting_tasks = Queue()
        # Round-robin
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[1]:
            self.waiting_tasks = {}
            for i in range(NUMBER_WORKERS):
                self.waiting_tasks[i+1] = Queue()
        # Priority-based
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[2]:
            self.waiting_tasks = PriorityQueue()
            
    def waiting_task_exists(self, worker_id):
        # Next-available
        if sys.argv[1] == SCHEDULING_ALGORITHMS[0]:
            return not self.waiting_tasks.empty()
        # Round-robin
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[1]:
            return not self.waiting_tasks[worker_id].empty()
        # Priority-based
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[2]:
            return not self.waiting_tasks.empty()
        else:
            raise Exception("Unreachable [waiting_task_exists]")

    def waiting_tasks_empty(self):
        if sys.argv[1] == SCHEDULING_ALGORITHMS[0] or sys.argv[1] == SCHEDULING_ALGORITHMS[2]:
            return self.waiting_tasks.empty()
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[1]:
            return all(queue.empty() for queue in self.waiting_tasks.values())
        else:
            raise Exception("Unreachable [waiting_tasks_empty]")
        
    def add_waiting_task(self, task_id, username, python_command, model_size, num_iterations, inference):
        if sys.argv[1] == SCHEDULING_ALGORITHMS[0]:
            self.waiting_tasks.put((task_id, username, python_command, model_size, inference))
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[1]:
            self.waiting_tasks[self.last_assigned_worker_id].put((task_id, username, python_command))
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[2]:
            self.waiting_tasks.push(Task(task_id, username, python_command, model_size, num_iterations, inference))
        
    def consume_message(self):
        self.messenger.consume()
        
    def next_available(self, model_size, inference_task):
        for worker_id in range(NUMBER_WORKERS, 0, -1):
            if worker_id not in [
                task["worker_id"] for task in self.ongoing_tasks.values()
            ] and (inference_task or model_size <= WORKER_MEMORY_SIZE[worker_id-1]):
                return worker_id
        return None
    
    def round_robin(self, model_size, inference_task):
        available_worker_count = NUMBER_WORKERS
        while True:
            next_worker_id = (self.last_assigned_worker_id % available_worker_count) + 1
            self.last_assigned_worker_id = next_worker_id
            if not inference_task and model_size <= WORKER_MEMORY_SIZE[next_worker_id - 1]:
                continue
            else:
                break
        
        if next_worker_id in [task["worker_id"] for task in self.ongoing_tasks.values()]:
            return None
        return next_worker_id

    # Find available worker
    def next_worker(self, model_size, inference_task):
        if sys.argv[1] in [SCHEDULING_ALGORITHMS[0], SCHEDULING_ALGORITHMS[2]]:
            return self.next_available(model_size, inference_task)
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[1]:
            return self.round_robin(model_size, inference_task)
        else:
            raise Exception("Unreachable [next_worker]")
        
    def schedule_waiting_task(self, free_worker_id):
        LOGGER.info("Scheduling waiting tasks.")
        if sys.argv[1] == SCHEDULING_ALGORITHMS[0]:
            waiting_tasks = []
            msg_sent = False
            
            LOGGER.info("Waiting Task Size: " + str(self.waiting_tasks.size()))
            
            while not self.waiting_tasks.empty():
                task_id, username, python_command, model_size, inference = self.waiting_tasks.get()
                if free_worker_id == self.next_worker(model_size, inference):
                    self.send_msg_to_worker(task_id, username, python_command, free_worker_id)
                    LOGGER.info("Task ID: " + str(waiting_task.task_id) + ", Model Size: " + str(waiting_task.size) + ", Worker ID: " + str(free_worker_id))
                    msg_sent = True
                    break
                else:
                    waiting_tasks.append((task_id, username, python_command, model_size, inference))
            
            for task_id, username, python_command, model_size, inference in waiting_tasks:
                self.waiting_tasks.push((task_id, username, python_command, model_size, inference))
                
            LOGGER.info("Waiting Task Size: " + str(self.waiting_tasks.size()))
            
            if not msg_sent:
                LOGGER.error("No available worker ID found for the next task.")
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[1]:
            task_id, username, python_command = self.waiting_tasks[free_worker_id].get()
            self.send_msg_to_worker(task_id, username, python_command, free_worker_id)
        elif sys.argv[1] == SCHEDULING_ALGORITHMS[2]:
            self.waiting_tasks.age_tasks()
            waiting_tasks = []
            msg_sent = False
            
            LOGGER.info("Waiting Task Size: " + str(self.waiting_tasks.size()))
            
            while not self.waiting_tasks.empty():
                waiting_task = self.waiting_tasks.pop()
                if free_worker_id == self.next_worker(waiting_task.size, waiting_task.inference):
                    self.send_msg_to_worker(waiting_task.task_id, waiting_task.username, waiting_task.python_command, free_worker_id)
                    LOGGER.info("Task ID: " + str(waiting_task.task_id) + ", Model Size: " + str(waiting_task.size) + ", Worker ID: " + str(free_worker_id))
                    msg_sent = True
                    break
                else:
                    waiting_tasks.append(waiting_task)
            
            for task in waiting_tasks:
                self.waiting_tasks.push(task)
                
            LOGGER.info("Waiting Task Size: " + str(self.waiting_tasks.size()))
            
            if not msg_sent:
                LOGGER.error("No available worker ID found for the next task.")
        else:
            raise Exception("Unreachable [scheduling]")
    
    def send_msg_to_worker(self, task_id, username, python_command, worker_id):
        self.ongoing_tasks[task_id] = {
            "username": username,
            "python_command": python_command,
            "worker_id": worker_id,
        }
        body = {
            "task_id": task_id,
            "username": username,
            "python_command": python_command,
            "worker_id": worker_id,
            "action": "start",
        }
        msg_to_worker = MessageBuilder.build(
            MessageCategory.scheduled_task, body, "OK"
        )
        self.messenger.produce_fanout(msg_to_worker)            
    
    # 1. If worker is available put task into ongoing_tasks & if not into waiting_tasks queue
    def new_tasks_scheduler(self):
        new_tasks = self.new_tasks  # key: task_id, {username, python_command}
        deleted = []
        for task_id in new_tasks:
            username = new_tasks[task_id]["username"]
            if "python_command" not in new_tasks[task_id]:
                continue
            python_command = new_tasks[task_id]["python_command"]
            model_size = new_tasks[task_id]["model_size"]
            num_iterations = new_tasks[task_id]["num_iterations"]
            inference = new_tasks["inference"]
            next_worker_id = self.next_worker(model_size, inference)

            # If available put task into ongoing_tasks & send message to the available worker
            if next_worker_id != None:
                self.send_msg_to_worker(task_id, username, python_command, next_worker_id)
                LOGGER.info("Task ID: " + str(task_id) + ", Model Size: " + str(model_size) + ", Worker ID: " + str(next_worker_id))
            # If not put task into waiting_tasks queue
            else:
                self.add_waiting_task(task_id, username, python_command, model_size, num_iterations, inference)
        
            deleted.append(task_id)

        for deleted_ in deleted:
            del new_tasks[deleted_]

    # 1-1. Execute new_tasks_scheduler periodically
    def start_new_tasks_scheduler(self):
        while True:
            self.new_tasks_scheduler()
            time.sleep(1)

    # 2. Consume message from API & workers
    def on_message_received(self, channel, method, body):
        category, msg_body = MessageBuilder.extract(body)

        # When API requested new task to scheduler (API message should contain "request")
        if category == MessageCategory.queue_request:
            # If start time is None, set start time as the time queue_request message is received
            self.elapsed_time_tracker.set_start_time()
            self.new_task_requested(msg_body)

        # When API uploaded file to FTP server (API message should contain "uploaded", "task_id": f"{task_id}")
        elif category == MessageCategory.queue_file_uploaded:
            self.new_file_uploaded(msg_body)

        # When worker sent task status to scheduler (worker message should contain "status")
        elif category == MessageCategory.task_status:
            self.worker_message_handler(msg_body)

    # 2-1. Consume message (new_task_request) from API, Create task_id & Add file to the new_tasks
    def new_task_requested(self, message):
        self.task_id += 1
        username = message["username"]

        self.new_tasks[self.task_id] = {"username": username}

        task_id_message = MessageBuilder.build(
            MessageCategory.queue_request_response,
            {"username": username, "task_id": self.task_id},
            "OK",
        )
        # Send task_id to API
        self.messenger.produce_fanout(task_id_message)
        LOGGER.info(f"Task ID {self.task_id} created and sent to API.")

    # 2-2. Consume message (new_file_uploaded) from API, Check new_file in FTP server & Send file_location to API

    def new_file_uploaded(self, message):
        task_id = message["task_id"]
        python_command = message["python_command"]
        client = FileTransferClient(
            host=HOST, port=21, username="kunwp1", password="test"
        )

        for task_id, task in self.new_tasks.items():
            # Send file location to worker server
            files = client.list_files("/" + task["username"] + "/" + str(task_id))

            if len(files):
                LOGGER.info(
                    f"Task ID {task_id} with Python command {python_command} was uploaded successfully."
                )
                self.new_tasks[task_id]["python_command"] = python_command
                self.new_tasks[task_id]["model_size"] = message["metrics"]["num_params"] * message["metrics"]["precision"]
                self.new_tasks[task_id]["num_iterations"] = message["metrics"]["num_iterations"]

    # 2-3. Consume message (task_status) from workers
    # 2-3. If task is completed, Delete from ongoing_tasks & Put waiting task into ongoing_tasks and send start message to worker
    def worker_message_handler(self, message):
        LOGGER.info(f"Message received: {message}")
        task_id = message["task_id"]
        status = message["status"]
        worker_id = message["worker_id"]

        # Put status message into worker_report_queue
        self.worker_report_queue.put(message)

        # If task is completed, delete from ongoing_tasks
        if status == 0:
            if task_id in self.ongoing_tasks:
                del self.ongoing_tasks[task_id]
                LOGGER.info(
                    f"Task ID {task_id} completed and removed from ongoing_tasks."
                )
                # Update end time every time a task is completed
                self.elapsed_time_tracker.update_end_time()
                # If there are no ongoing tasks and waiting tasks, measure total elapsed time
                if not self.ongoing_tasks and self.waiting_tasks_empty():
                    total_elapsed_time = self.elapsed_time_tracker.measure_elapsed_time()
                    if total_elapsed_time is not None:
                        LOGGER.info(f"Total elapsed time for all tasks: {total_elapsed_time}")
                    else:
                        LOGGER.info("Elapsed time measurement error or start time not set.")

                # If waiting task exists, find available worker & put the task into ongoing_tasks & send message to the available worker
                if self.waiting_task_exists(worker_id):
                    self.schedule_waiting_task(worker_id)
        else:
            LOGGER.error("An error occurred.")

def scheduler_main():
    scheduler = Scheduler()
    scheduler.consume_message()
    scheduler.start_new_tasks_scheduler()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise Exception("Scheduling algorithm should be specified as an argument: {}".format(SCHEDULING_ALGORITHMS))
    if sys.argv[1] not in SCHEDULING_ALGORITHMS:
        raise Exception("Scheduling algorithm should be one of the following arguments: {}".format(SCHEDULING_ALGORITHMS))
    scheduler_main()
