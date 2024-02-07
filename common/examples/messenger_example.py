import time
import logging

from cs230_common.messenger import PikaMessenger

logging.basicConfig(level=logging.INFO)

HOST = "18.222.25.84"
PORT = "5673"
TOPICs = ["node_1_scheduling"]

def main():
    messenger = PikaMessenger(
        broker_host=HOST,
        broker_port=PORT,
        receive_topics=TOPICs
    )
    
    # async consume
    messenger.consume()

    count = 0
    try:
        while True:
            count+=1
            messenger.produce(f"{count} message sent.", topic="file")
            time.sleep(5)
    except KeyboardInterrupt:
        messenger.stop_consuming()
    finally:
        del messenger

if __name__ == "__main__":
    main()