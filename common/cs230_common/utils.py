from enum import StrEnum, auto
import json


class MessageCategory(StrEnum):
    heartbeat = auto()
    queue_request = auto()
    queue_request_response = auto()
    queue_file_uploaded = auto()
    report = auto()
    scheduled_task = auto()
    task_status = auto()

class Channels(StrEnum):
    sdk_scheduler = auto()
    worker_scheduler = auto()
    api_to_scheduler = auto()


class MessageBuilder:
    @staticmethod
    def build(category: MessageCategory, body: str, status: str = "OK") -> dict:
        message = {
            "CATEGORY": category,
            "status": status,
            "body": body,
        }
        return json.dumps(message)

    @staticmethod
    def extract(message: str):
        message = json.loads(message)

        if message["status"] != "OK":
            raise RuntimeError(f"Error: {message['status']}")

        return message["CATEGORY"], message["body"]
