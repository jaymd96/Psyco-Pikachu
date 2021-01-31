from abc import ABC, abstractmethod
from threading import Event

import pika
from tenacity import retry, retry_if_exception_type


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
                print("[rabbit-producer] Got NOTIFY:", notify.pid, notify.channel, notify.payload)
                print("[rabbit-producer] Sending message to create a queue")
                try:
                    self.publish(notify.payload)
                except Exception as e:
                    print(e)
                
                # Check for termination 
                if notify is _sentinel: 
                    in_q.put(_sentinel) 
                    break
            except IndexError:
                print("[rabbit-producer] Nothing to Process")
                event.wait(5)
                continue
            except Exception as e:
                continue
