import aioredis  # pip install aioredis==2


def get_asyn_redis(host=None, port=None, password=None, db:int=None, max_connections=10):
    """ 获取异步reids连接 """
    kwargs = dict(
        port=port,
        db=db,
        max_connections=max_connections,
        encoding="utf-8",
        decode_responses=True
    )
    if password:
        kwargs['password'] = password
    pool = aioredis.ConnectionPool.from_url(f"redis://{host}",**kwargs)
    return aioredis.Redis(connection_pool=pool)

def get_asyn_redis_pool(host=None, port=None, password=None, db:int=None, max_connections=10):
    """ 获取异步reids连接 """
    kwargs = dict(
        port=port,
        db=db,
        max_connections=max_connections,
        encoding="utf-8",
        decode_responses=True
    )
    if password:
        kwargs['password'] = password
    pool = aioredis.ConnectionPool.from_url(f"redis://{host}",**kwargs)
    return pool
