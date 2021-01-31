from typing import Callable
from functools import wraps

class ConnectionRecovery():
    def connected(self) -> bool:
        raise NotImplementedError

    def connect(self):
        raise NotImplementedError

    def close(self):
        raise NotImplementedError

def reconnect(recoverable_exception: Exception):
    def reconnect_exception(f: Callable):
        @wraps(f)
        def wrapper(connector, *args, **kwargs):
            if not connector.connected():
                connector.connect()

            try:
                return f(connector, *args, **kwargs)
            except recoverable_exception as e:
                raise e
            except:
                connector.close()
                raise

        return wrapper
    return reconnect_exception