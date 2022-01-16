#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
'''
@File   :   redis.py
@Time   :   2021/12/27 17:32
@Author :   Blank
@Version:   2.0
@Desc   :   Redis pcblib
'''

import glob
import os
from typing import Any, Dict, Union

import aioredis  # # pip install aioredis==2
from aioredis.connection import ConnectionPool
from loguru import logger


def get_asyn_redis(host: str = '127.0.0.1', port: int = 6379, password: str = None, db: Union[str, int] = 0, max_connections: int = 32, **kwargs):
    """ 使用连接池创建reids客户端 """
    conf = dict(
        port=port,
        db=db,
        max_connections=max_connections,
        encoding="utf-8",
        decode_responses=True
    )
    if password:
        conf['password'] = password
    pool = aioredis.ConnectionPool.from_url(f"redis://{host}",**conf)
    return AsyncRedisDB(connection_pool=pool, **kwargs)

class AsyncRedisDB(aioredis.Redis):

    def __init__(self, *, host: str = "localhost", port: int = 6379, db: Union[str, int] = 0, password: str = None, socket_timeout: float = None, socket_connect_timeout: float = None, socket_keepalive: bool = None, socket_keepalive_options: Dict[str, Any] = None, connection_pool: ConnectionPool = None, unix_socket_path: str = None, encoding: str = "utf-8", encoding_errors: str = "strict", decode_responses: bool = False, retry_on_timeout: bool = False, ssl: bool = False, ssl_keyfile: str = None, ssl_certfile: str = None, ssl_cert_reqs: str = "required", ssl_ca_certs: str = None, ssl_check_hostname: bool = False, max_connections: int = None, single_connection_client: bool = False, health_check_interval: int = 0, client_name: str = None, username: str = None):
        """ 继承 aioredis.Redis 的封装类"""
        super().__init__(host=host, port=port, db=db, password=password, socket_timeout=socket_timeout, socket_connect_timeout=socket_connect_timeout, socket_keepalive=socket_keepalive, socket_keepalive_options=socket_keepalive_options, connection_pool=connection_pool, unix_socket_path=unix_socket_path, encoding=encoding, encoding_errors=encoding_errors, decode_responses=decode_responses, retry_on_timeout=retry_on_timeout, ssl=ssl, ssl_keyfile=ssl_keyfile, ssl_certfile=ssl_certfile, ssl_cert_reqs=ssl_cert_reqs, ssl_ca_certs=ssl_ca_certs, ssl_check_hostname=ssl_check_hostname, max_connections=max_connections, single_connection_client=single_connection_client, health_check_interval=health_check_interval, client_name=client_name, username=username)
        self.init_scripts()

    def init_scripts(self, script_dir=None):
        self._scripts = {}
        if not script_dir:
            script_dir = os.path.join(os.path.dirname(__file__), 'scripts')
        for filename in glob.glob(os.path.join(script_dir, '*.lua')):
            with open(filename, 'r') as fh:
                script_obj = self.register_script(fh.read())
                script_name = os.path.splitext(os.path.basename(filename))[0]
                self._scripts[script_name] = script_obj

    async def zset_set_by_score(self, name: str, min: int, max: int, score: int, num: int = '') -> list:
        """ 获取指定区间数据并修改分数为指定值 """

        script = self.register_script("""
            local key = KEYS[1]
            local min_score = ARGV[1]
            local max_score = ARGV[2]
            local set_score = ARGV[3]
            local num = ARGV[4]

            -- get zset data
            local datas = nil
            if num ~= '' then
                datas = redis.call('zrangebyscore', key, min_score, max_score, 'withscores', 'limit', 0, num)
            else
                datas = redis.call('zrangebyscore', key, min_score, max_score, 'withscores')
            end

            -- set score
            local item_list = {}
            for i=1, #datas, 2 do
                table.insert(item_list, datas[i])
                redis.call('zadd', key, set_score, datas[i])
            end
            return item_list
        """)
        return await script(keys=[name], args=[min, max, score, num])

    async def zset_increase_by_score(self, name: str, min: int, max: int, increment: int, num: int = '') -> list:
        """ 获取指定区间数据并修改分数加上增量 """

        script = self.register_script("""
            local key = KEYS[1]
            local min_score = ARGV[1]
            local max_score = ARGV[2]
            local increase = ARGV[3]
            local num = ARGV[4]

            -- get zset data
            local datas = nil
            if num ~= '' then
                datas = redis.call('zrangebyscore', key, min_score, max_score, 'withscores', 'limit', 0, num)
            else
                datas = redis.call('zrangebyscore', key, min_score, max_score, 'withscores')
            end

            -- set score
            local item_list = {}
            for i=1, #datas, 2 do
                table.insert(item_list, datas[i])
                redis.call('zincrby', key, increase, datas[i])
            end
            return item_list
        """)
        return await script(keys=[name], args=[min, max, increment, num])

    async def zset_del_by_score(self, name: str, min: int, max: int, num: int = '') -> list:
        """ 获取指定区间数据并删除 """

        script = self.register_script("""
            local key = KEYS[1]
            local min_score = ARGV[1]
            local max_score = ARGV[2]
            local num = ARGV[3]

            -- get zset data
            local datas = nil
            if num ~= '' then
                datas = redis.call('zrangebyscore', key, min_score, max_score, 'withscores', 'limit', 0, num)
            else
                datas = redis.call('zrangebyscore', key, min_score, max_score, 'withscores')
            end

            -- del data
            local item_list = {}
            for i=1, #datas, 2 do
                table.insert(item_list, datas[i])
                redis.call('zrem', key, datas[i])
            end
            return item_list
        """)
        return await script(keys=[name], args=[min, max, num])

    async def zset_fetch_by_sorce(self, name: str,  min: int, max: int, num: int = None) -> list:
        """ 获取指定区间数据 """
        return await self.zrangebyscore(name, min, max, 0, num)

    async def _write_zset(self, name:str, data:list, score:int=0):
        """ 批量写入zset """
        result = await self.zadd(name, dict.fromkeys(data, score))
        logger.debug(f'redis:{name} | insert {result} | updata {len(data)-result} | total {len(data)}')
        return result

    async def write(self, key_type: str, name: str, data: Union[str, list], score: int = 0):
        """ 写入操作 """
        if key_type == 'zset':
            return await self._write_zset(name, data, score)
        else:
            logger.error(f'param error | key_type:{key_type} | correct in [str,list,set,zset,hash]')

    async def get_conut(self, name:str):
        """ 获取数据量 """

        key_type = await self.type(name)
        if key_type == 'list':
            return await self.llen(name)
        elif key_type == 'set':
            return await self.scard(name)
        elif key_type == 'zset':
            return await self.zcard(name)
        elif key_type == 'string':
            return await self.strlen(name)
        elif key_type == 'hash':
            return await self.hlen(name)
        else:
            return False

    async def getter(self, name: str, page_size:int = 500):
        """ 获取key全部数据 page_size批量获取针对list,set,zset有效"""

        key_type = await self.type(name)

        if key_type == 'string':
            yield await self.get(name)

        elif key_type == 'list':
            i = 0
            while i := i+1:
                item_list =  await self.lrange(name, page_size * (i-1), page_size * i - 1)
                if len(item_list) < page_size:
                    break
                yield item_list
            yield item_list

        elif key_type == 'set':
            item_list = []
            async for item in self.scan_iter(name):
                item_list.append(item)
                if len(item_list) == page_size:
                    yield item_list
                    item_list = []
            yield item_list

        elif key_type == 'zset':
            item_list = []
            async for item in self.zscan_iter(name):
                item_list.append(item)
                if len(item_list) == page_size:
                    yield item_list
                    item_list = []
            yield item_list

        elif key_type == 'hash':
            yield await self.hgetall(name)

        else:
            yield False
