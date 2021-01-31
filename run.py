from collections import deque
from configparser import ConfigParser
from threading import Event, Thread

from src.consumer.postgresClient import PostgresClient
from src.producer.simpleProducer import SimpleProducor
from src.utils import make_rabbit_url

# Object that signals shutdown 
_sentinel = object() 

config = ConfigParser()
config.read('./private/config.ini') 

rabbit_url = make_rabbit_url(**config['rabbitmq'])
rabbit_client = SimpleProducor(rabbit_url)
db_client = PostgresClient(config['postgres'])


if __name__ == "__main__":
    q = deque()
    t1 = Thread(target = rabbit_client.send_events, args =(q,), daemon=True) 
    t2 = Thread(target = db_client.poll_events, args =(q,), daemon=True) 
    try:
        t1.start() 
        t2.start()
    except:
        q.append(_sentinel) 
    
    t1.join()
    t2.join()