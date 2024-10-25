from tred.main import Locks, Tred

def test_execute():
    r = Tred(decode_responses=True, redis_lock_execution_timeout=6, redis_lock_blocking_timeout=6)
    r.execute("set", "tj", "k", lock_type=Locks.DISTRIBUTED)
    r.execute("set", "tj", "mika", lock_type=Locks.DISTRIBUTED)
    r.execute("hset", "caps", "b", "B", mapping={"c": "C"}, lock_type=Locks.LOCAL)
    r.execute("hset", "caps", "d", "D", mapping={"e": "E"})
    r.execute("hset", "caps", "a", "A", lock_type = Locks.LOCAL)
    assert r.execute("hget", "caps", "e", lock_type=Locks.LOCAL) == "E"
    assert r.execute("hget", "caps", "f", lock_type=Locks.LOCAL) == None
    assert r.execute("hget", "caps", "a", lock_type = Locks.DISTRIBUTED) == "A"
    assert r.execute("hget", "caps", "b", lock_type = Locks.LOCAL) == "B"
    with r.r.lock(name="own"):
        r.execute("set", "k", "u")
        r.execute("set", "z", "a")
        assert r.execute("get", "z") == "a"
        assert r.execute("get", "k") == "u"

