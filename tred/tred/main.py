import logging
from enum import Enum, unique
from redis import Redis
from threading import Lock, Thread
from typing import Any, Dict, Union
from tred.utils.singleton import singleton

logger = logging.getLogger(__name__)
logging.basicConfig(format='%(asctime)s, %(levelname)s, [%(filename)s:%(lineno)d] Message: %(message)s', datefmt='%Y-%m-%d:%H:%M:%S', level=logging.INFO)

@unique
class Locks(Enum):
    NONE = 1
    LOCAL = 2
    DISTRIBUTED = 3

@singleton
class Tred:
    def __init__(self, *args, redis_lock_execution_timeout: float = .1, redis_lock_blocking_timeout: float = .1, prometheus_histogram_object: Union[Any, None] = None, **kwargs) -> Any:
        self.r = Redis(*args, **kwargs)
        self.local_locks_map:Dict = {}
        self.redis_lock_execution_timeout: float = redis_lock_execution_timeout
        self.redis_lock_blocking_timeout: float = redis_lock_blocking_timeout
        self.prometheus_histogram_object: Union[Any, None] = prometheus_histogram_object

    def execute(self, f_name: str, *args, lock_type: Locks = Locks.NONE, lock_name: str = "", **kwargs):
        f = getattr(self.r, f_name)
        ret = None
        match lock_type:
            case Locks.NONE:
                if self.prometheus_histogram_object:
                    with self.prometheus_histogram_object.labels(f_name).time():
                        return f(*args, **kwargs)
                else:
                    return f(*args, **kwargs)
            case Locks.LOCAL:
                if not lock_name:
                    lock_name = f_name
                if lock_name not in self.local_locks_map:
                    self.local_locks_map[lock_name] = Lock()
                with self.local_locks_map[lock_name]:
                    # print(f"acquired distributed lock for {lock_name}")
                    if self.prometheus_histogram_object:
                        with self.prometheus_histogram_object.labels(f_name).time():
                            return f(*args, **kwargs)
                    else:
                        return f(*args, **kwargs)
            case Locks.DISTRIBUTED:
                if not lock_name:
                    lock_name = f_name
                with self.r.lock(name=lock_name, timeout=self.redis_lock_execution_timeout, blocking_timeout=self.redis_lock_blocking_timeout):
                    # print(f"acquired distributed lock for {lock_name}")
                    if self.prometheus_histogram_object:
                        with self.prometheus_histogram_object.labels(f_name).time():
                            return f(*args, **kwargs)
                    else:
                        return f(*args, **kwargs)
            case _:
                msg = f"Unknown lock type -> '{lock_type}', supported locks:" + (", ".join([f'Locks.{x.name}' for x in Locks]))
                logger.error(msg)
                return None
        return ret

def main():
    o = Tred(decode_responses=True, redis_lock_execution_timeout=6, redis_lock_blocking_timeout=6)
    print(o.execute("set", "tj", "mika", lock_type=Locks.DISTRIBUTED))
    print(o.execute("get", "tj", lock_type=Locks.DISTRIBUTED))
    print(o.execute("get", "tj", lock_type="Something"))
    with o.r.lock(name="test"):
        print(o.execute("set", "k", "u"))
        print(o.execute("set", "z", "a"))
        print(o.execute("get", "z"))
        print(o.execute("get", "k"))

if __name__ == '__main__':
    main()

