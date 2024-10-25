import time
from enum import Enum, unique
from redis import Redis
from threading import Lock, Thread
from typing import Any, Dict
from tred.utils.singleton import singleton

@unique
class Locks(Enum):
    NONE = 1
    LOCAL = 2
    DISTRIBUTED = 3

@singleton
class Tred:
    def __init__(self, *args, redis_lock_execution_timeout: float = .1, redis_lock_blocking_timeout: float = .1, **kwargs) -> Any:
        self.r = Redis(*args, **kwargs)
        self.local_locks_map:Dict = {}
        self.redis_lock_execution_timeout: float = redis_lock_execution_timeout
        self.redis_lock_blocking_timeout: float = redis_lock_blocking_timeout

    def execute(self, f_name: str, *args, lock_type: Locks = Locks.NONE, lock_name: str = "", **kwargs):
        f = getattr(self.r, f_name)
        ret = None
        if lock_type == Locks.NONE:
            #print("No lock")
            ret = f(*args, **kwargs)
        elif lock_type == Locks.LOCAL:
            if not lock_name:
                lock_name = f_name
            if lock_name not in self.local_locks_map:
                self.local_locks_map[lock_name] = Lock()
            with self.local_locks_map[lock_name]:
                # print(f"acquired local lock for {lock_name}")
                time.sleep(1)
                ret = f(*args, **kwargs)
        elif lock_type == Locks.DISTRIBUTED:
            if not lock_name:
                lock_name = f_name
            with self.r.lock(name=lock_name, timeout=self.redis_lock_execution_timeout, blocking_timeout=self.redis_lock_blocking_timeout):
                # print(f"acquired distributed lock for {lock_name}")
                ret = f(*args, **kwargs)
        else:
            print("Unknown lock type, supported locks:", (", ".join([f'Locks.{x.name}' for x in Locks])))
        return ret


def main():
    r = Tred(decode_responses=True, redis_lock_execution_timeout=6, redis_lock_blocking_timeout=6)
    print(r.execute("set", "tj", "mika", lock_type=Locks.DISTRIBUTED))
    print(r.execute("get", "tj", lock_type=Locks.DISTRIBUTED))
    with r.r.lock(name="own"):
        r.execute("set", "k", "u")
        r.execute("set", "z", "a")
        print(r.execute("get", "z"))
        print(r.execute("get", "k"))

if __name__ == '__main__':
    main()

