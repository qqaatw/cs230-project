import os
import time
import logging
import traceback

from ..messenger import PikaMessenger
from ..utils import MessageCategory, Channels, MessageBuilder, TrainingResult
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

    def _wait_response(self, expected_category, timeout: float = 5.0):
        start = time.time()
        response = {}
        while (time.time() - start) < timeout:
            if not self.messenger.q.empty():
                response = self._verify_response(
                    self.messenger.q.get()[1], expected_category
                )
                if response != {}:
                    break
        if response == {}:
            raise TimeoutError("The reponse time out.")
        return response

    def _verify_response(
        self, response: str, expected_category: MessageCategory
    ) -> dict:
        category, body = MessageBuilder.extract(response)

        if category != expected_category:
            return {}

        return body

    def _check_results(self, task_id: int) -> str:
        """Retrieve results from the file server. Return a bool indicting succees or not for now."""
        client = FileTransferClient(
            self.ftp_host, self.ftp_port, self.username, self.password
        )
        files = client.list_files(f"{task_id}/")
        if f"{task_id}/results" in files:
            results = client.list_files(f"{task_id}/results")
            for result in results:
                if result.endswith(".pt"):
                    return TrainingResult.completed
            return TrainingResult.error
        return TrainingResult.in_progress

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

    def report(self, model_path: str, tfevent_path: str | None):
        """Upload the trained model and logs to the file server.

        Parameters
        ----------
        model_path : str

        tfevent_path : str
        """

        client = FileTransferClient(
            self.ftp_host, self.ftp_port, self.username, self.password
        )

        client.upload_results(
            self.get_task_id(),
            [model_path, tfevent_path] if tfevent_path else [model_path],
        )
        print("Results uploaded")

        message = MessageBuilder.build(
            MessageCategory.report, {"task_id": self.get_task_id()}
        )

        self.messenger.produce(message, Channels.worker_scheduler)

    def request_scheduling(self) -> str:
        """Request a task id"""

        message = MessageBuilder.build(
            MessageCategory.queue_request, {"username": self.username}
        )
        self.messenger.produce(message, "api_to_scheduler")
        body = self._wait_response("queue_request_response")

        assert "task_id" in body

        return body["task_id"]

    def upload_task(
        self,
        task_id: int,
        file_list: list[str],
        python_command: str,
        metrics: dict,
        inference: bool,
    ):
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
                "metrics": metrics,
                "inference": inference,
            },
        )
        self.messenger.produce(message, Channels.api_to_scheduler)

    def measure_parameters(self, model):
        return {
            "num_params": sum(p.numel() for p in model.parameters()),
            "precision": next(model.parameters()).element_size(),
        }

    def get_scheduling_status(self): ...

    @staticmethod
    def launch(sdk, fn: callable, *args, **kwargs):
        """Launch the main program with error handling.

        Parameters
        ----------
        fn : callable
            main function.
        """

        try:
            try:
                fn(sdk, *args, **kwargs)
            except:
                traceback.print_exc()
            finally:
                sdk.messenger.stop_consuming()
        except:
            ...
