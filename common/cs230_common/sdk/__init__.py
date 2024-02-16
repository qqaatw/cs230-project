import time
import json
import logging

from ..messenger import PikaMessenger
from ..utils import MessageCategory, Channels, MessageBuilder
from ..file_transfer_client import FileTransferClient

LOGGER = logging.getLogger(__name__)

class SDK:
    def __init__(self,
        username : str,
        password : str,
        broker_host : str,
        broker_port : int,
        receive_topics : list[str],
        ftp_host : str,
        ftp_port : int):
        
        self.username = username
        #self.password = password

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
    
    def _wait_response(self, timeout : float = 5.0):
        start = time.time()
        while self.messenger.q.empty() and (time.time() - start) < timeout:
            pass
        if self.messenger.q.empty():
            raise TimeoutError("The reponse time out.")
        return self.messenger.q.get()
    
    def _verify_response(self, response: str, expected_category: MessageCategory):
        obj = json.loads(response)
        
        assert "CATEGORY" in obj, "CATEGORY field is not in the response"

        if obj["CATEGORY"] != expected_category:
            raise RuntimeError(f"The expected category is {expected_category}, but the received one is {obj['CATEGORY']}")
        
        if obj["status"] != "OK":
            raise RuntimeError(f"Error: {obj['status']}")
        
        return obj["body"]
            

    def heartbeat(self):
        """Heartbeat 
        """
        self.messenger.produce(r'{"CATEGORY" : MessageCategory., "body" : {} }')
        #self.
    
    def complete(self, model_path : str, tfevent_path : str):
        """Upload the trained model and logs to the file server.

        Parameters
        ----------
        model_path : str

        tfevent_path : str
        """
        ...

    def request_scheduling(self) -> tuple[int, str]:
        """Request a task id"""

        message = MessageBuilder.build(MessageCategory.queue_request, 
            {
                "username" : self.username,
            }
        )
        self.messenger.produce(message, Channels.sdk_scheduler)
        body = self._verify_response(self.wait_response())
        
        assert "task_id" in body

        return body["task_id"], body["path"]
    
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

        client = FileTransferClient(self.ftp_host, self.ftp_port, self.username, self.password)
        client.push_file(task_id, file_list)

        message = MessageBuilder.build(MessageCategory.queue_file_uploaded, 
            {
                "username" : self.username,
                "task_id" : task_id,
                "python_command" : python_command
            }
        )
        self.messenger.produce(message, Channels.sdk_scheduler)

    def get_scheduling_status(self):
        ...
    


