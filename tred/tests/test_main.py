from tred import Locks, Tred
from prometheus_client import Histogram

h = Histogram('request_latency_seconds', 'Description of histogram')
o = Tred(decode_responses=True, redis_lock_execution_timeout=6, redis_lock_blocking_timeout=6, prometheus_histogram_object=h)
p = Tred(decode_responses=True, redis_lock_execution_timeout=6, redis_lock_blocking_timeout=6)

def test_execute():
    assert o.execute("set", "tj", "k", lock_type=Locks.DISTRIBUTED) == True
    assert o.execute("set", "tj", "mika", lock_type=Locks.DISTRIBUTED) == True
    assert o.execute("hset", "caps", "b", "B", mapping={"c": "C"}, lock_type=Locks.LOCAL) == 0
    assert o.execute("hset", "caps", "d", "D", mapping={"e": "E"}) == 0
    assert o.execute("hset", "caps", "a", "A", lock_type = Locks.LOCAL) == 0
    assert o.execute("hget", "caps", "e", lock_type=Locks.LOCAL) == "E"
    assert o.execute("hget", "caps", "f", lock_type=Locks.LOCAL) == None
    assert o.execute("hget", "caps", "a", lock_type = Locks.DISTRIBUTED) == "A"
    assert o.execute("hget", "caps", "b", lock_type = Locks.LOCAL) == "B"
    assert o.execute("hget", "caps", "b", lock_type = "random string") == None
    with o.r.lock(name="own"):
        assert o.execute("set", "k", "u") == True
        assert o.execute("set", "z", "a") == True
        assert o.execute("get", "z") == "a"
        assert o.execute("get", "k") == "u"

    assert p.execute("set", "tj", "k", lock_type=Locks.DISTRIBUTED) == True
    assert p.execute("set", "tj", "mika", lock_type=Locks.DISTRIBUTED) == True
    assert p.execute("hset", "caps", "b", "B", mapping={"c": "C"}, lock_type=Locks.LOCAL) == 0
    assert p.execute("hset", "caps", "d", "D", mapping={"e": "E"}) == 0
    assert p.execute("hset", "caps", "a", "A", lock_type = Locks.LOCAL) == 0
    assert p.execute("hget", "caps", "e", lock_type=Locks.LOCAL) == "E"
    assert p.execute("hget", "caps", "f", lock_type=Locks.LOCAL) == None
    assert p.execute("hget", "caps", "a", lock_type = Locks.DISTRIBUTED) == "A"
    assert p.execute("hget", "caps", "b", lock_type = Locks.LOCAL) == "B"
    assert p.execute("hget", "caps", "b", lock_type = "random string") == None
    with p.r.lock(name="own"):
        assert p.execute("set", "k", "u") == True
        assert p.execute("set", "z", "a") == True
        assert p.execute("get", "z") == "a"
        assert p.execute("get", "k") == "u"

