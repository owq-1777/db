#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
'''
@File   :   mongo.py
@Time   :   2021/09/03 11:35
@Author :   Blank
@Version:   1.0
@Desc   :   Mongo async class
'''

import asyncio
import time
import traceback
from logging import Logger
from typing import List

import motor.motor_asyncio
from loguru import logger
from pymongo import UpdateOne, database
from pymongo.collection import Collection
from pymongo.cursor import Cursor
from pymongo.mongo_client import MongoClient


class MongoTool:

    @staticmethod
    async def get_total_count(collect: Collection, filter: dict = {}):
        """ Get the number of documents according to the filter. """
        # 没有设置过滤器时, 使用 estimated_document_count() 查询大型集合会更快
        return await collect.count_documents(filter) if filter else await collect.estimated_document_count()


class MongoBase:

    def __init__(self, config: dict, *, db_name: str = None, collect_name: str = None, is_async=False,logger: Logger=logger) -> None:
        """ Mongo 连接数据库基类

        :Parameters:
            - `config`: 连接配置
            示例:
            {
                'host': 127.0.0.1,
                'port': 27017,
                'password': '123456',
                'username': 'admin',
                'database': 'admin'
            }
            - `db`: 连接数据库, 默认使用config中数据库
            - `collect`: 连接的采集表名, 默认为空
            - `is_async`: 是否使用异步连接, 默认False
        """
        self.conn_str = self.get_conn_str(config)
        self.client = self.get_async_client(self.conn_str) if is_async else self.get_client(self.conn_str)
        self.db: database.Database = self.client[config['database']] if db_name is None else self.client[db_name]
        self.collect = None if collect_name is None else self.db[collect_name]
        self.logger = logger

    @staticmethod
    def get_conn_str(config: dict) -> str:
        """get mongo connection string  """
        if config.get('password'):
            if config.get('database'):
                return 'mongodb://{username}:{password}@{host}:{port}/{database}'.format(**config)
            return 'mongodb://{username}:{password}@{host}:{port}'.format(**config)
        else:
            return 'mongodb://{host}:{port}'.format(**config)

    @staticmethod
    def get_client(conn_str: str):
        return MongoClient(conn_str)

    @staticmethod
    def get_async_client(conn_str: str, timeout: int = 5000):
        """ create mongo async client """
        return motor.motor_asyncio.AsyncIOMotorClient(conn_str, serverSelectionTimeoutMS=timeout)

    def write(self, collect_name:str, data: List[dict], retry=5):
        """ 批量写入文档 """

        collect = self.db[collect_name]
        documents = [UpdateOne({'_id': each['_id']}, {'$set': each}, upsert=True) for each in data]

        for i in range(retry):
            try:
                resp = collect.bulk_write(documents)
                self.logger.info(f'success updata {resp.upserted_count}, matched {resp.matched_count} documents to {collect.full_name}, total write {len(documents)}')
                return resp
            except Exception as e:
                if i + 1 == retry:
                    self.logger.error('write lose! {e}')
                    raise
                time.sleep(2)


class MongoGetter:

    def __init__(self, collect: Collection, logger: Logger = logger,
                 cursor: Cursor = None, filter: dict = None, return_fields: list = None,
                 total_cnt: int = None, page_size: int = 500, retry: int = 5) -> None:
        """ Mongo 分页查询异步迭代器 """
        self.collect = collect
        self.logger = logger

        self.filter = filter or {}
        self.projection = dict.fromkeys(return_fields, 1) if return_fields else {}
        if not self.projection.get('_id'):
            self.projection['_id'] = 0

        self.cursor = cursor if cursor else self.collect.find(self.filter, self.projection)
        self.total_cnt = total_cnt  # 取的数据总量, 默认取全部
        self.page_size = page_size  # 单次返回的数据量
        self.fetch_cnt = 0          # 已获取的数据量
        self.retry = retry          # 获取失败重试次数

    async def get_data(self):
        data = []

        if self.total_cnt is None:
            self.total_cnt = await MongoTool.get_total_count(self.collect)

        async for document in self.cursor:

            data.append(document)
            self.fetch_cnt += 1

            if len(data) >= self.page_size or self.fetch_cnt >= self.total_cnt:
                return data

        raise StopAsyncIteration

    async def __anext__(self, retry=1):
        try:
            return await self.get_data()

        except StopAsyncIteration:
            raise StopAsyncIteration
        except Exception:
            self.logger.error(traceback.format_exc())

            if self.retry > retry:
                await asyncio.sleep(2)
                return await self.__anext__(retry + 1)
            else:
                raise

    def __aiter__(self):
        return self


class AsyncMongo(MongoBase):

    def __init__(self, config: dict, collect_name: str=None, db_name: str = None, *,  logger: Logger = logger) -> None:
        super().__init__(config, db_name=db_name, collect_name=collect_name, is_async=True)

    def geteer(self, collect_name: str = None, filter: dict = None, return_fields: list = None, total_cnt: int = None, page_size: int = 500, retry: int = 5):
        """ 异步迭代查询 """
        collect = self.db[collect_name] if collect_name else self.collect
        return MongoGetter(collect, logger=self.logger, filter=filter, return_fields=return_fields,
                           total_cnt=total_cnt, page_size=page_size, retry=retry)

    async def write(self, collect_name:str, documents: List[dict], retry:int=5):
        """ 批量写入文档 """

        collect = self.db[collect_name]
        documents = [UpdateOne({'_id': each['_id']}, {'$set': each}, upsert=True) for each in documents]

        for i in range(retry):
            try:
                resp = await collect.bulk_write(documents)
                self.logger.info(f'success updata {resp.upserted_count}, matched {resp.matched_count} documents to {collect.full_name}, total write {len(documents)}')
                return resp
            except Exception as e:
                if i + 1 == retry:
                    self.logger.error('write lose! {e}')
                    raise
                await asyncio.sleep(2)

    async def find_one(self, collect_name, filter, retry=3, raise_error=True):
        try:
            data = await self.db[collect_name].find_one(filter)
            return data
        except:
            if retry:
                return await self.find_one(collect_name, filter, retry - 1, raise_error)
            elif raise_error:
                raise

    async def fetch_all(self, collect_name, filter, return_fields=None, retry=3, raise_error=True):
        data = []
        cursor = self.db[collect_name].find(filter, return_fields)
        for i in range(retry):
            try:
                async for document in cursor:
                    data.append(document)
            except Exception as e:
                self.logger.error(e)
                if i + 1 >= retry and raise_error:
                    raise
            else:
                break
        await cursor.close()
        return data
