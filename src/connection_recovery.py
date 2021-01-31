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
        def wrapper(storage, *args, **kwargs):
            if not storage.connected():
                storage.connect()

            try:
                return f(storage, *args, **kwargs)
            except recoverable_exception as e:
                raise e
            except:
                storage.close()
                raise

        return wrapper
    return reconnect_exception