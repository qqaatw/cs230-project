import time
import logging

import json
import os
from queue import Queue

from cs230_common.messenger import PikaMessenger
from cs230_common.file_transfer_client import FileTransferClient
from cs230_common.utils import *
from measure_elapsed_time import Elapsed_Time_Tracker

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

HOST = "18.119.97.104"
PORT = "5673"
TOPICS = ["api_to_scheduler", "worker_scheduler"]


class Scheduler:
    def __init__(self):
        self.new_tasks = {}  # key: task_id, {username, python_command}
        self.ongoing_tasks = {}  # key: task_id, {username, python_command, worker_id}
        self.waiting_tasks_queue = Queue()
        self.worker_report_queue = Queue()
        self.messenger = PikaMessenger(
            broker_host=HOST,
            broker_port=PORT,
            receive_topics=TOPICS,
            callback=self.on_message_received,
        )
        self.task_id = 0
        self.elapsed_time_tracker = Elapsed_Time_Tracker()

    def consume_message(self):
        self.messenger.consume()

    # Find available worker
    def find_available_worker(self, ongoing_tasks):
        for worker_id in range(1, 4):
            if worker_id not in [
                task["worker_id"] for task in self.ongoing_tasks.values()
            ]:
                return worker_id
        return None

    # 1. If worker is available put task into ongoing_tasks & if not into waiting_tasks queue
    def new_tasks_scheduler(self):
        new_tasks = self.new_tasks  # key: task_id, {username, python_command}
        deleted = []
        for task_id in new_tasks:
            username = new_tasks[task_id]["username"]
            if "python_command" not in new_tasks[task_id]:
                continue
            python_command = new_tasks[task_id]["python_command"]
            available_worker_id = self.find_available_worker(self.ongoing_tasks)

            # If available put task into ongoing_tasks & send message to the available worker
            if available_worker_id:
                self.ongoing_tasks[task_id] = {
                    "username": username,
                    "python_command": python_command,
                    "worker_id": available_worker_id,
                }
                start_new_tasks_msg = MessageBuilder.build(
                    MessageCategory.scheduled_task,
                    {
                        "task_id": task_id,
                        "username": username,
                        "python_command": python_command,
                        "worker_id": available_worker_id,
                        "action": "start",
                    },
                    "OK",
                )
                self.messenger.produce_fanout(start_new_tasks_msg)
                print("Debug 1")
                deleted.append(task_id)

            # If not put task into waiting_tasks queue
            else:
                self.waiting_tasks_queue.put((task_id, username, python_command))
                print("Debug 2")

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
            self.new_task_requested(msg_body)
            # If start time is None, set start time as the time queue_request message is received
            self.elapsed_time_tracker.set_start_time()

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

    # 2-3. Consume message (task_status) from workers
    # 2-3. If task is completed, Delete from ongoing_tasks & Put waiting task into ongoing_tasks and send start message to worker
    def worker_message_handler(self, message):
        LOGGER.info(f"Message received: {message}")
        task_id = message["task_id"]
        status = message["status"]

        # Put status message into worker_report_queue
        self.worker_report_queue.put(message)

        # If task is completed, delete from ongoing_tasks
        if status == 0:
            # Everytime scheduler receives termination message, update the end time
            self.elapsed_time_tracker.update_end_time()
            if task_id in self.ongoing_tasks:
                del self.ongoing_tasks[task_id]
                LOGGER.info(
                    f"Task ID {task_id} completed and removed from ongoing_tasks."
                )

                # If there are no ongoing or waiting tasks, measure total elapsed time
                if not self.ongoing_tasks and self.waiting_tasks_queue.empty():
                    total_elapsed_time = self.elapsed_time_tracker.measure_elapsed_time()
                    if total_elapsed_time is not None:
                        print(f"Total elapsed time for all tasks: {total_elapsed_time}")
                    else:
                        LOGGER.info("Elapsed time measurement error of start time not set.")

                # If waiting task exists, find available worker & put the task into ongoing_tasks & send message to the available worker
                elif not self.waiting_tasks_queue.empty():
                    LOGGER.info("Scheduling new task.")
                    (
                        next_task_id,
                        username,
                        python_command,
                    ) = self.waiting_tasks_queue.get()
                    available_worker_id = self.find_available_worker(self.ongoing_tasks)
                    if available_worker_id is not None:
                        self.ongoing_tasks[next_task_id] = {
                            "username": username,
                            "python_command": python_command,
                            "worker_id": available_worker_id,
                        }
                        start_next_task_body = json.dumps(
                            {
                                "task_id": next_task_id,
                                "username": username,
                                "python_command": python_command,
                                "worker_id": available_worker_id,
                                "action": "start",
                            }
                        )
                        start_next_task_msg = MessageBuilder.build(
                            MessageCategory.scheduled_task, start_next_task_body, "OK"
                        )
                        self.messenger.produce_fanout(start_next_task_msg)
                    else:
                        LOGGER.error("No available worker ID found for the next task.")

        elif status == 1:
            LOGGER.error("An error occurred.")


def scheduler_main():
    scheduler = Scheduler()
    scheduler.consume_message()
    scheduler.start_new_tasks_scheduler()


if __name__ == "__main__":
    scheduler_main()
