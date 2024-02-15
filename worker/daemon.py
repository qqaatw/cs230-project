from cs230_common.messenger import PikaMessenger
import sys
import json
import subprocess
import logging
import os

logging.basicConfig(level = logging.DEBUG)

SHELL = False
if sys.platform.startswith('win'):
    SHELL = True

class CondaManager():
    def __init__(self, name: str, path: str):
        self.name = name
        self.path = path

    def check_env_existing(self, delete: bool = True):
        result = subprocess.run(f"conda env list".split(" "), capture_output=True, text=True, shell=SHELL)
        for line in result.stdout.split('\n'):
            if line.split(' ')[0] == self.name:
                if delete:
                    subprocess.run(f"conda env remove -n {self.name} -y".split(" "), shell=SHELL)
                    return False
                return True
        return False
    
    def build(self):
        self.check_env_existing(delete=True)
        mkdir = f"mkdir env".split()
        if os.path.exists('env'):
            raise RuntimeError("Directory `env` already exists.")
    
        print(mkdir)
        result = subprocess.run(mkdir, capture_output=True, text=True, shell=SHELL)
        command = f"conda env create --prefix env -f {self.path}".split(" ")
        print(command)
        result = subprocess.run(command, capture_output=True, text=True, shell=SHELL)
        print(result.stdout, result.stderr)
        
        if not self.check_env_existing(delete=False):
            raise RuntimeError("Environment doesn't exist.")

def main():
    with open("config.json", "r") as f:
        config = json.load(f)
    
    #conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    #conda_manager.build()

    consumer = PikaMessenger(
        broker_host=config["broker"]["broker_host"],
        broker_port=config["broker"]["broker_port"],
        topics= [v for v in config["broker"]["topics"].values()])

    consumer.consume()

def test():
    with open("config.json", "r") as f:
        config = json.load(f)
    conda_manager =CondaManager(config["env"]["name"], config["env"]["path"])
    conda_manager.build()

if __name__ == "__main__":
    #main()
    test()