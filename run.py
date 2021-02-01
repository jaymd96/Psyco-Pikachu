from collections import deque
from configparser import ConfigParser
from threading import Event, Thread
from concurrent.futures import ThreadPoolExecutor

from src.consumer.postgresClient import PostgresClient
from src.producer.simpleProducer import SimpleProducor
from src.utils import make_rabbit_url

NUM_RABBIT_WORKERS = 10

## CONFIG
config = ConfigParser()
config.read('./private/config.ini') 

## SETUP
rabbit_url = make_rabbit_url(**config['rabbitmq'])
db_client = PostgresClient(config['postgres'])

_sentinel = object() 
event = Event()

if __name__ == "__main__":
    q = deque()
    try:
        rabbit_executor = ThreadPoolExecutor(max_workers=NUM_RABBIT_WORKERS)
        for worker_numebr in range(1,NUM_RABBIT_WORKERS):
            rabbit_executor.submit(SimpleProducor(rabbit_url).send_events, event, q)
        t2 = Thread(target = db_client.poll_events, args =(event, q, ), daemon=True) 
        t2.start()
    except KeyboardInterrupt:
        rabbit_executor.shutdown()
        q.append(_sentinel)