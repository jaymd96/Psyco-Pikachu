import sys
import json
import logging
from threading import Event
import pika
from pika.exceptions import AMQPConnectionError, AMQPError
from abc import ABC, abstractmethod

from tenacity import retry, retry_if_exception_type

from connection_recovery import reconnect

import utils
logger = logging.getLogger(__name__)


class Producor(ABC):
    """
    Base class for producors.
    """
    _connection = None
    _channel = None

    def __init__(self, url):
        """
        :url: amqp url to connect to rabbitmq
        """
        self._url = url
        self._namespace = "psycho-pikachu"
        self.connect()

    def connected(self) -> bool:
        if self._connection.is_closed or self._channel.is_closed:
            return False
        else:
            try:
                self._connection.process_data_events()
                return True
            except pika.exceptions.ConnectionClosed:
                logger.info("Connection closed by server, try reconnect.")
                return False


    def connect(self):
        self._connection = pika.BlockingConnection(pika.URLParameters(self._url))
        self._channel = self._connection.channel()
        self._channel.confirm_delivery()
        return self._channel

    def close(self):
        if self.connected():
            try:
                self._connection.close()
            except Exception:
                pass

        self._channel = None       
        self._connection = None
    
    @abstractmethod
    def publish(self, message, **kwargs):
        pass

    def send_events(self, in_q):
        event = Event()
        while True: 
            # Get some data 
            try:
                notify = in_q.popleft()
                print("Got NOTIFY:", notify.pid, notify.channel, notify.payload)
                print("Sending message to create a queue")
                try:
                    self.publish(notify.payload)
                except Exception as e:
                    print(e)
                
                # Check for termination 
                if notify is _sentinel: 
                    in_q.put(_sentinel) 
                    break
            except IndexError:
                print("[Consumer] Nothing to Process")
                event.wait(5)
                continue
            except Exception as e:
                continue


class SimpleProducor(Producor):
    """
    SimpleProducor use singleton pattern. Connection to rabbitmq is BlockingConnection. All messages are durable and ack needed.
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
        
        exchange = utils.make_exchange_name(namespace, self.EXCHANGE_TYPE)
        routing_key = utils.make_direct_key(namespace)
        queue_name = utils.make_queue_name(namespace, self.EXCHANGE_TYPE)

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
            

class BroadCaster(Producor):
    """
    Almost the same as SimpleProducor, the only differences are exchange type is fanout, and routing key is ignored.(for now)
    """
    EXCHANGE_TYPE = "fanout"

    def publish(self, message, to_hub=None, namespace=None):
        """
        Publish a message to all subscribers.
        :message: the message to publish, dict.
        :to_hub: the message hub name, where the publisher publish message to.
        """
        assert isinstance(message, dict), "Only dict type is supported for a message."
        body = json.dumps(message)
        if namespace is None:
            namespace = self._namespace
        exchange = utils.make_exchange_name(namespace, self.EXCHANGE_TYPE, extra=to_hub)
        channel.exchange_declare(exchange=exchange, exchange_type=self.EXCHANGE_TYPE, durable=True)
        return channel.basic_publish(exchange=exchange, routing_key="", body=body)

# ToDo Topic Publisher