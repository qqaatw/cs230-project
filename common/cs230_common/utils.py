from enum import StrEnum, auto

class MessageCategory(StrEnum):
    heartbeat = auto()
    queue_request = auto()
    queue_request_response = auto()
    queue_file_uploaded = auto()
    report = auto()
    scheduled_task = auto()

class Channels(StrEnum):
    sdk_scheduler = auto()
    worker_scheduler = auto()

class MessageBuilder:
    @staticmethod
    def build(category : MessageCategory, body : str, status : str = "OK") -> dict:
        message = {
            "CATEGORY": category,
            "status" : status,
            "body" : body,
        }
        return message