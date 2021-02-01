import json
from abc import ABC, abstractmethod
from threading import Event

import pika
from src.utils import *
from src.connection_recovery import reconnect
from pika.exceptions import AMQPConnectionError, AMQPError
from tenacity import retry, retry_if_exception_type
from .producerBase import Producor


class SimpleProducor(Producor):
    """
    SimpleProducor, BlockingConnection. Messages are durable and ack needed.
    """
    EXCHANGE_TYPE = "direct"

    @retry(retry=retry_if_exception_type(AMQPConnectionError))
    @reconnect(AMQPConnectionError)
    def publish(self, message, namespace=None):
        """
        The put/get or put/listen pattern. Put a message into the queue.
        :message: the message to put in queue, dict.
        """
        if namespace is None:
            namespace = self._namespace
        
        exchange = make_exchange_name(namespace, self.EXCHANGE_TYPE)
        routing_key = make_direct_key(namespace)
        queue_name = make_queue_name(namespace, self.EXCHANGE_TYPE)

        channel = self._channel

        channel.exchange_declare(exchange=exchange, exchange_type=self.EXCHANGE_TYPE, durable=True)
        channel.queue_declare(queue=queue_name, durable=True)  # make sure the queue is created.
        binding_key = routing_key  # direct mode only match routing_key with binding_key, they should be the same
        channel.queue_bind(queue_name, exchange, binding_key)
        return channel.basic_publish(
            exchange=exchange,
            routing_key=routing_key,
            body=message,
            properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"))
 