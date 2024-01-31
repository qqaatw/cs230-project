import json
import pika
import time
import threading
import functools
import subprocess
import logging

logging.basicConfig(level = logging.DEBUG)

class PikaMassenger():

    def __init__(self, broker_host, broker_port, topics):
        self.conn = pika.BlockingConnection(pika.ConnectionParameters(host=broker_host, port=broker_port))
        self.channel = self.conn.channel()
        self.topics = topics
        for topic in topics:
            self.channel.queue_declare(queue=topic)
    
    def callback(ch, method, properties, body):
        print(" [x] %r:%r consumed" % (method.routing_key, body))

    def consume(self):
        result = self.channel.queue_declare('', auto_delete=True)
        #queue_name = result.method.queue
        #for key in keys:
            #self.channel.queue_bind(
                #exchange=self.exchange_name, 
                #queue=queue_name,
                #routing_key=key)

        for topic in self.topics:
            print(f"topic: {topic}")
            self.channel.basic_consume(
                queue=topic, 
                on_message_callback=self.callback,
                auto_ack=True)
            
        print("Start consuming")
        self.channel.start_consuming()

class CondaManager():
    def __init__(self, name, path):
        self.name = name
        self.path = path

    def check_env_existing(self, delete=True):
        result = subprocess.run(f"conda env list".split(" "), capture_output=True, text=True)
        for line in result.stdout.split('\n'):
            if line.split(' ')[0] == self.name:
                if delete:
                    subprocess.run(f"conda env remove -n {self.name} -y".split(" "))
                    return False
                return True
        return False
    
    def build(self):
        self.check_env_existing(delete=True)
        command = f"conda env create -f {self.path}".split(" ")
        print(command)
        result = subprocess.run(command, capture_output=True, text=True)
        print(result.stdout, result.stderr)
        
        if not self.check_env_existing(self, delete=False):
            raise RuntimeError("Environment doesn't exist.")



def main():
    with open("config.json", "r") as f:
        config = json.load(f)
    
    conda_manager = CondaManager(config["env"]["name"], config["env"]["path"])
    conda_manager.build()

    consumer = PikaMassenger(broker_host=config["broker"]["broker_host"], broker_port=config["broker"]["broker_port"], topics= [v for v in config["broker"]["topics"].values()])
    consumer_thread = threading.Thread(target=consumer.consume, daemon=None)
    consumer_thread.start()



    try:
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        consumer.channel.stop_consuming()
        consumer.conn.close()

def test():
    with open("config.json", "r") as f:
        config = json.load(f)
    conda_manager =CondaManager(config["env"]["name"], config["env"]["path"])
    conda_manager.build()

if __name__ == "__main__":
    #main()
    test()