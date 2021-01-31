import psycopg2
import select
from connection_recovery import ConnectionRecovery, reconnect
from tenacity import retry, retry_if_exception_type


class DbStorage(ConnectionRecovery):

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
    def poll_events(self, out_q):
        conn = self._connection
        conn.notifies = out_q

        curs = conn.cursor()
        curs.execute("LISTEN test;")

        print("[pg_consumer] Waiting for notifications on channel 'test'")
        while True:
            try:
                if select.select([conn],[],[],5) == ([],[],[]):
                    print("[pg_consumer] Timeout")
                else:
                    conn.poll()
            except Exception as e:
                raise e