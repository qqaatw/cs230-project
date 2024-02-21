import os
import time
import json
import logging

from ..messenger import PikaMessenger
from ..utils import MessageCategory, Channels, MessageBuilder
from ..file_transfer_client import FileTransferClient

LOGGER = logging.getLogger(__name__)


class SDK:
    def __init__(
        self,
        username: str,
        password: str,
        broker_host: str,
        broker_port: int,
        receive_topics: list[str],
        ftp_host: str,
        ftp_port: int,
    ):
        self.username = username
        self.password = password

        self.ftp_host = ftp_host
        self.ftp_port = ftp_port

        self.messenger = PikaMessenger(
            broker_host=broker_host,
            broker_port=broker_port,
            receive_topics=receive_topics,
        )
        self.messenger.consume()

    def __del__(self):
        self.messenger.stop_consuming()

    def __login(self, username: str, password: str) -> bool:
        """(Unused for now) Login to the system.

        Parameters
        ----------
        username : str
            User name.
        password : str
            Password.

        Returns
        -------
        bool
            if True, the user is successfully logged in.
        """
        # ftp =  FTP()
        ...

    def _wait_response(self, timeout: float = 5.0):
        start = time.time()
        while self.messenger.q.empty() and (time.time() - start) < timeout:
            pass
        if self.messenger.q.empty():
            raise TimeoutError("The reponse time out.")
        return self.messenger.q.get()

    def _verify_response(self, response: str, expected_category: MessageCategory):
        print(response)
        obj = json.loads(response)

        assert "CATEGORY" in obj, "CATEGORY field is not in the response"

        if obj["CATEGORY"] != expected_category:
            raise RuntimeError(
                f"The expected category is {expected_category}, but the received one is {obj['CATEGORY']}"
            )

        if obj["status"] != "OK":
            raise RuntimeError(f"Error: {obj['status']}")

        return obj["body"]

    def device(self):
        import torch

        if torch.cuda.is_available():
            device = torch.device("cuda")
            print("Using CUDA")
        elif torch.backends.mps.is_available():
            device = torch.device("mps")
            print("Using MPS")
        else:
            device = torch.device("cpu")
            print("Using CPU")
        return device

    def get_task_id(self):
        return os.environ["TASK_ID"]

    def heartbeat(self, task_id: int, timestamp: int):
        """Heart beat

        Parameters
        ----------
        task_id : int
            Task ID.
        timestamp : int
            Timestamp
        """

        message = MessageBuilder.build(
            MessageCategory.heartbeat, {"task_id": task_id, "timestamp": time.time()}
        )
        self.messenger.produce(message, Channels.sdk_scheduler)

    def report(self, task_id: int, model_path: str, tfevent_path: str | None):
        """Upload the trained model and logs to the file server.

        Parameters
        ----------
        model_path : str

        tfevent_path : str
        """

        client = FileTransferClient(
            self.ftp_host, self.ftp_port, self.username, self.password
        )
        client.push_file(task_id, [model_path, tfevent_path] if tfevent_path else [model_path])

        message = MessageBuilder.build(MessageCategory.report, {"task_id": task_id})

        self.messenger.produce(message, Channels.worker_scheduler)

    def request_scheduling(self) -> str:
        """Request a task id"""

        message = MessageBuilder.build(
            MessageCategory.queue_request, {"username": self.username}
        )
        self.messenger.produce(message, "api_to_scheduler")
        queue_name, body = self._wait_response()
        body = self._verify_response(body, "queue_request_response")

        assert "task_id" in body

        return body["task_id"]

    def upload_task(self, task_id: int, file_list: list[str], python_command: str):
        """Upload user code and notify the scheduler.

        Parameters
        ----------
        task_id : int
            Task ID.
        file_list : list[str]
            File list.
        python_command : str
            The entry point.
        """

        client = FileTransferClient(
            self.ftp_host, self.ftp_port, self.username, self.password
        )
        client.push_file(task_id, file_list)

        message = MessageBuilder.build(
            MessageCategory.queue_file_uploaded,
            {
                "username": self.username,
                "task_id": task_id,
                "python_command": python_command,
            },
        )
        self.messenger.produce(message, Channels.api_to_scheduler)

    def get_scheduling_status(self):
        ...
    
    def launch(self, fn : callable):
        """Launch the main program with error handling.

        Parameters
        ----------
        fn : callable
            main function.
        """

        try:
            fn(self)
        except KeyboardInterrupt:
            self.messenger.stop_consuming()
