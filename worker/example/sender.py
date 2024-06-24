# import conda.cli.python_api as run_command

import pika

connection = pika.BlockingConnection(pika.ConnectionParameters("18.223.152.106", 5673))
channel = connection.channel()

channel.queue_declare(queue="node_1_scheduling")
channel.queue_declare(queue="file")

channel.basic_publish(exchange="", routing_key="node_1_scheduling", body="Hello World!")
print(" [x] Sent 'Hello World!'")
channel.basic_publish(exchange="", routing_key="file", body="File sent!")
print(" [x] Sent 'File sent!'")
connection.close()
