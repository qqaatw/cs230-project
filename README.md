# CS230 Project - Distributed Job Scheduling System in Machine Learning Clusters

## Setup

**For scheduler and worker**

1. Install conda virtual environment.
2. Run `conda env create --prefix cs230 -f configuration.yaml` under `worker` directory.
3. Activate the environment.
4. Install common library with `pip install -e .` command under `common` directory.
5. Install `torch` and `torchvision` with `pip`.

**For FTP server and RabbitMQ broker**

Deploy with docker from docker hub:

`rabbitmq:3.12-management`

`garethflowers/ftp-server`

**For users**

1. Install common library with `pip install -e .` command under `common` directory.

## `config.json`

The FTP server and broker host can be set up in the `worker/config.json` file.

## Launch

**Scheduler**

There are three scheduling algorithm available:

```python scheduler.py [next-available, round-robin, priority-based]```

e.g.

```python scheduler.py next-available```

**Worker**

```python daemon.py [worker_id]```

e.g.

```python daemon.py 1```

## Workflow

Please look at `tester.py` to learn how a user should send a new task request to scheduler and retrieve the results.

