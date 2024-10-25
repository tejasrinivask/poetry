from threading import Lock

def singleton(cls):
    instances = {}
    lock = Lock()
    def get_instance(*args, **kwargs) -> object:
        with lock:
            if cls not in instances:
                instances[cls] = cls(*args, **kwargs)
            return instances[cls]
    return get_instance

