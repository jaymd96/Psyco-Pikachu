import psycopg2
import select
from src.connection_recovery import Connector, reconnect
from tenacity import retry, retry_if_exception_type
from src.log.LogMeta import LogMeta


class PostgresClient(Connector, metaclass=LogMeta):

    def __init__(self, conn: dict):
        self._connection_params: dict = conn
        self._connection = None

    def connected(self) -> bool:
        return self._connection and self._connection.closed == 0

    def connect(self):
        self.close()
        self._connection = psycopg2.connect(**self._connection_params)
        self._connection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    def close(self):
        if self.connected():
            try:
                self._connection.close()
            except Exception:
                pass

        self._connection = None

    @retry(retry=retry_if_exception_type(psycopg2.Error))
    @reconnect(psycopg2.Error)
    def poll_events(self, event, out_q):
        conn = self._connection
        conn.notifies = out_q

        curs = conn.cursor()
        curs.execute("LISTEN test;")
        self._PostgresClient__logger.info("Waiting for notifications on channel 'test'")
        while True:
            try:
                if select.select([conn],[],[],5) == ([],[],[]):
                    self._PostgresClient__logger.info("Timeout")
                else:
                    conn.poll()
            except Exception as e:
                raise e