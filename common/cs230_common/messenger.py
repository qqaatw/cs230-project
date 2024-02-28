import pika
import threading
import logging
import functools
import queue

LOGGER = logging.getLogger(__name__)


class PikaMessenger:
    """PikaMessager for RabbitMQ communications.

    PikaMesseneger by default subscribes to a fan out exchange which serves as the public channel for every clinet connecting to the broker.

    Parameters
    ----------
    broker_host : str
        Host.
    broker_port : str
        Port.
    receive_topics : list[str]
        List of subscribed topics.
    callback : callable, optional
        Custom callback.
    """

    def __init__(
        self,
        broker_host: str,
        broker_port: str,
        receive_topics: list[str],
        callback: callable = None,
    ):
        self.conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=broker_host, port=broker_port)
        )
        self.channel = self.conn.channel()
        self.publish_conn = pika.BlockingConnection(
            pika.ConnectionParameters(host=broker_host, port=broker_port)
        )
        self.publish_conn_channel = self.publish_conn.channel()
        self._callback = self._default_callback if callback is None else callback
        self.receive_topics = receive_topics
        for topic in receive_topics:
            self.channel.queue_declare(queue=topic)
        self.thread = None
        self.threads = []
        self.q = queue.Queue()

        # publishing to fanout exchange
        self.publish_conn_channel.exchange_declare(
            exchange="public", exchange_type="fanout"
        )

        # receiving from fanout exchange
        result = self.channel.queue_declare(queue="", exclusive=True)
        self.public_queue_name = result.method.queue
        self.channel.queue_bind(exchange="public", queue=self.public_queue_name)

    def __del__(self):
        LOGGER.info("Close connections")
        for thread in self.threads:
            thread.join()

        if self.thread is not None:
            self.thread.join()
        self.conn.close()
        self.publish_conn.close()

    def _ack_message(self, channel, delivery_tag):
        # Ridiculous
        """Note that `channel` must be the same pika channel instance via which
        the message being ACKed was retrieved (AMQP protocol constraint).
        """

        if channel.is_open:
            channel.basic_ack(delivery_tag)
        else:
            # Channel is already closed, so we can't ACK this message;
            # log and/or do something that makes sense for your app in this case.
            pass

    def _do_work(self, channel, method_frame, body):
        # Ridiculous
        thread_id = threading.get_ident()
        fmt1 = "Thread id: {} Delivery tag: {} Message body: {}"
        LOGGER.info(fmt1.format(thread_id, method_frame.delivery_tag, body))
        self._callback(channel, method_frame, body)
        cb = functools.partial(self._ack_message, channel, method_frame.delivery_tag)
        self.conn.add_callback_threadsafe(cb)

    def _on_message(self, channel, method_frame, header_frame, body):
        t = threading.Thread(target=self._do_work, args=(channel, method_frame, body))
        t.start()
        self.threads.append(t)

    def _default_callback(self, ch, method, body):
        self.q.put((method.routing_key, body))

    def _consume(self):
        for topic in self.receive_topics:
            LOGGER.info(f"topic: {topic}")
            self.channel.basic_consume(
                queue=topic, on_message_callback=self._on_message
            )

        self.channel.basic_consume(
            queue=self.public_queue_name, on_message_callback=self._on_message
        )

        LOGGER.info("Start consuming")
        self.channel.start_consuming()
        self.publish_conn_channel.start_consuming()

    def produce(self, message: str, topic: str, exahange: str = ""):
        self.publish_conn_channel.basic_publish(
            exchange=exahange, routing_key=topic, body=message
        )

    def produce_fanout(self, message: str):
        self.publish_conn_channel.basic_publish(
            exchange="public", routing_key="", body=message
        )

    def consume(self):
        self.thread = threading.Thread(target=self._consume, daemon=None)
        self.thread.start()

    def stop_consuming(self):
        LOGGER.info("Stop consuming")
        self.channel.stop_consuming()
