from enum import StrEnum, auto
import json
from argparse import ArgumentParser


class MessageCategory(StrEnum):
    heartbeat = auto()
    queue_request = auto()
    queue_request_response = auto()
    queue_file_uploaded = auto()
    report = auto()
    scheduled_task = auto()
    task_status = auto()
    profile = auto()


class Channels(StrEnum):
    sdk_scheduler = auto()
    worker_scheduler = auto()
    api_to_scheduler = auto()


class TrainingResult(StrEnum):
    completed = auto()
    error = auto()
    in_progress = auto()


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

        assert "CATEGORY" in message, "CATEGORY field is not in the response"

        if message["status"] != "OK":
            raise RuntimeError(f"Error: {message['status']}")

        return message["CATEGORY"], message["body"]


def get_general_parser() -> ArgumentParser:
    parser = ArgumentParser(description="General parser", add_help=False)
    group = parser.add_argument_group("General")
    group.add_argument("--account", type=str, default="kunwp1", help="Acccount")
    group.add_argument("--password", type=str, default="test", help="Password")
    return parser


def get_hyperparameters_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Hyperparameter parser", add_help=False)
    group = parser.add_argument_group("Hyperparameters")
    group.add_argument("--num_classes", type=int, default=10, help="Number of classes")
    group.add_argument("--num_epochs", type=int, default=1, help="Number of epochs")
    group.add_argument("--batch_size", type=int, default=64, help="Batch size")
    group.add_argument(
        "--inference", action="store_true", help="Whether the task is inference only"
    )
    group.add_argument(
        "--learning_rate", type=float, default=0.001, help="Learning rate"
    )
    group.add_argument("--model", type=str, default="resnet18", help="Model")
    return parser
