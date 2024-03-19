import random
import time
from argparse import ArgumentParser

from cs230_common.sdk import SDK
from cs230_common.file_transfer_client import FileTransferClient
from cs230_common.utils import (
    get_hyperparameters_parser,
    get_general_parser,
    TrainingResult,
)
from torchvision.models import resnet18, resnet34, resnet50

BROKER_HOST = "18.119.97.104"
BROKER_PORT = 5673

model_map = {
    "resnet18": resnet18,
    "resnet34": resnet34,
    "resnet50": resnet50,
}

max_log_len = 0
num_dot = 0


def args_to_str(args):
    return " ".join((f"--{k} {v}" for k, v in vars(args).items()))


def print_log(log, ending="\r"):
    global num_dot
    global max_log_len
    log += "." * num_dot

    max_log_len = max(len(log), max_log_len)

    print(" " * max_log_len, end=ending)
    print(log, end=ending)

    num_dot = 0 if num_dot == 3 else num_dot + 1


def main(sdk: SDK, args, general_args, hyperparameter_args):
    client = FileTransferClient(BROKER_HOST, 21, args.account, args.password)
    client.erase_files()

    if len(args.models) == 0:
        args.models = random.choices(list(model_map.keys()), k=args.num_tasks)

    if args.inference:
        inference_flags = [True for _ in range(args.num_tasks)]
    else:
        inference_flags = random.choices([True, False], k=args.num_tasks)


    assert (
        args.num_tasks == len(args.models)
    ), f"The number of tasks ({args.num_tasks}) should match the number of models ({len(args.models)})"

    task_ids = set()

    for idx, (model_name, inference) in enumerate(zip(args.models, inference_flags), 1):
        model = model_map[model_name]()
        metrics = sdk.measure_parameters(model)
        metrics.update({"num_iterations": args.num_epochs})
        hyperparameter_args.inference = inference

        print(f"ID: {idx} Model Name: {model_name} Metrics: {metrics} Inference: {inference}")
        
        task_id = sdk.request_scheduling()
        task_ids.add(task_id)
        python_command = " ".join(
            (
                "python resnset_train.py",
                args_to_str(general_args),
                args_to_str(hyperparameter_args),
            )
        )
        sdk.upload_task(
            task_id=task_id,
            file_list=["resnset_train.py"],
            python_command=python_command,
            metrics=metrics,
            inference=hyperparameter_args.inference,
        )
        time.sleep(args.task_creating_interval)

    error_ids = set()
    completed_ids = set()
    while len(task_ids) > 0:
        for task_id in task_ids:
            status = sdk._check_results(task_id)
            if status == TrainingResult.completed:
                completed_ids.add(task_id)
            elif status == TrainingResult.error:
                error_ids.add(task_id)

        task_ids -= completed_ids
        task_ids -= error_ids

        log = (
            f"({completed_ids}) completed, ({error_ids}) error, in progress: {task_ids}"
        )

        print_log(log)

        time.sleep(0.5)

    print(f"({completed_ids}) completed, ({error_ids}) error, in progress: {task_ids}")


if __name__ == "__main__":
    parser = ArgumentParser(
        parents=[get_general_parser(), get_hyperparameters_parser()]
    )
    subparsers = parser.add_subparsers()
    general_subparser = subparsers.add_parser(
        "General Parser", parents=[get_general_parser()]
    )
    hyperparameter_subparser = subparsers.add_parser(
        "Hyperparameter Parser", parents=[get_hyperparameters_parser()]
    )
    group = parser.add_argument_group("Testing")

    group.add_argument("--num_tasks", type=int, default=5)
    group.add_argument("--task_creating_interval", type=float, default=1.0)
    group.add_argument(
        "--models",
        nargs="+",
        type=str,
        default=[],
        choices=list(model_map.keys()),
        help="List of model names",
    )
    group.add_argument("--erase", type=bool, default=True)
    group.add_argument("--precision", type=int, default=4)

    args = parser.parse_args()
    general_args, _ = general_subparser.parse_known_args()
    hyperparameter_args, _ = hyperparameter_subparser.parse_known_args()

    sdk = SDK(
        args.account,
        args.password,
        broker_host=BROKER_HOST,
        broker_port=BROKER_PORT,
        receive_topics=[],
        ftp_host=BROKER_HOST,
        ftp_port=21,
    )

    try:
        main(sdk, args, general_args, hyperparameter_args)
    finally:
        sdk.messenger.stop_consuming()
