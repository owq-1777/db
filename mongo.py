#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
'''
@File   :   mongo_async.py
@Time   :   2021/12/21 16:19
@Author :   Blank
@Version:   2.0
@Desc   :   Mongo pcblib
'''

from typing import Sequence

import motor.motor_asyncio
import pymongo
from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError, ConnectionFailure
from pymongo.operations import DeleteOne, InsertOne, UpdateOne
from pymongo.results import BulkWriteResult


class MongoDB:

    def __init__(self, url: str = None, host: str = 'localhost', port: int = 27017,
                 database: str = 'admin', username: str = None, password: str = None, **kwargs) -> None:

        if url:
            self.client = MongoClient(url, **kwargs)
        else:
            if password != None and username != None:
                self.client = MongoClient(host=host, port=port, username=username, password=password, authSource=database, **kwargs)
            else:
                self.client = MongoClient(host=host, port=port, **kwargs)

        self.db = self.client.get_database(database)

    def __str__(self) -> str:
        return f'db: {self.db}'

    def get_database(self, database_name: str):
        """ Get database object. """
        return self.client.get_database(database_name)

    def get_collection(self, coll_name: str, **kwargs):
        """ Get collection object. """
        return self.db.get_collection(coll_name, **kwargs)


class AsyncMongoDB:

    def __init__(self, url: str = None, host: str = 'localhost', port: int = 27017,
                 database: str = 'admin', username: str = None, password: str = None, **kwargs):
        """Get a database by url or conf.

        Args:
            host (str, optional): host. Defaults to 'localhost'.
            port (int, optional): port. Defaults to 27017.
            database (str, optional): database name. Defaults to 'admin'.
            username (str, optional): user name. Defaults to None.
            password (str, optional): password. Defaults to None.
            url (str, optional): mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]]. Defaults to None.
        """

        if url:
            self.client = self.get_client(url, **kwargs)
        else:
            if password != None and username != None:
                self.client = self.get_client(host=host, port=port, username=username, password=password, authSource=database, **kwargs)
            else:
                self.client = self.get_client(host=host, port=port, **kwargs)

        self.db = self.get_database(database)

    def __str__(self) -> str:
        return f'db: {self.db}'


# ------------------------------------  ------------------------------------ #

    @staticmethod
    def get_client(*args, **kwargs):
        """ Get mongo async client. """
        return motor.motor_asyncio.AsyncIOMotorClient(*args, **kwargs)

    def get_database(self, database_name: str) -> Database:
        """ Get database object. """
        return self.client[database_name]

    def get_collection(self, coll_name: str, **kwargs) -> Collection:
        """ Get collection object. """
        return self.db.get_collection(coll_name, **kwargs)

    async def get_collection_list(self) -> list:
        """ Get collection names list"""
        return await self.db.list_collection_names()

# ------------------------------------  ------------------------------------ #

    async def create_index(self, coll_name: str, keys: Sequence, sort: int = 1, unique=True):
        """Creates an index on this collection.

        Args:
            coll_name ([type]): collection name
            keys (Sequence): The key that creates the index(Single or compound)
            sort (int, optional): 1=Ascending, -1=Descending. Defaults to 1.
            unique (bool, optional): [description]. Defaults to True.

        """
        coll = self.get_collection(coll_name)
        sort_type = pymongo.ASCENDING if sort == 1 else pymongo.DESCENDING
        _keys = [(key, sort_type) for key in keys]

        await coll.create_index(_keys, unique=unique)

    async def get_index_info(self, coll_name: str):
        """ Get collection index information. """
        return await self.get_collection(coll_name).index_information()

    async def del_collection(self, coll_name: str):
        """ Delete specified collection. """
        return await self.db.drop_collection(coll_name)

    async def get_count(self, coll_name: str, filter: dict = {}):
        """ Get the number of collection documents. """
        collect = self.get_collection(coll_name)
        return await collect.count_documents(filter) if filter else await collect.estimated_document_count()

    async def write(self, coll_name: str, documents: list[dict]) -> bool:
        """Batch write documents.

        Args:
            coll_name (str): collection name.
            documents (list[dict]): write documents.
        Returns:
            bool: operating result.
        """
        if not documents:
            logger.warning('documents is null')
            return True

        # 有_id则更新数据 无则插入
        operate_list = [UpdateOne({'_id': item['_id']}, {'$set': item}, upsert=True) if item.get('_id') != None else InsertOne(item) for item in documents]

        try:
            collect = self.get_collection(coll_name)
            result: BulkWriteResult = await collect.bulk_write(operate_list, ordered=False)
            logger.info(f'mongo:{collect.full_name} | insert {result.inserted_count} | updata {result.upserted_count} | modified {result.matched_count} | total {len(documents)}')
            return True
        except BulkWriteError as e:
            logger.error(e.details)
            return False

    async def delect(self, coll_name: str, documents: list[dict]) -> bool:
        """Delete mongo document data

        Args:
            coll_name (str):  collection name.
            documents (list[dict]): delect documents.

        Returns:
            bool: operating result.
        """

        if not documents:
            logger.warning('documents is null')
            return True

        # 根据_id删除数据
        operate_list = [DeleteOne({'_id': i['_id']}) for i in documents if i.get('_id')]

        try:
            collect = self.get_collection(coll_name)
            result: BulkWriteResult = await collect.bulk_write(operate_list, ordered=False)
            logger.info(f'mongo:{collect.full_name} | deleted {result.deleted_count} | total {len(documents)}')
            return True
        except BulkWriteError as e:
            logger.error(e.details)
            return False


    async def getter(self, coll_name: str, filter: dict = {}, return_fields: list = None,
                     return_cnt: int = 'all', page_size: int = 500):
        """ Batch get document builder.

        Args:
            coll_name (str): collection name.
            filter (dict, optional): filter condition. Defaults to None.
            return_fields (list, optional): select the fields to return. Defaults to None.
            return_cnt (int, optional): getting document total quantity. Defaults to all.
            page_size (int, optional): quantity returned each time. Defaults to 500.

        Yields:
            Iterator[list[dict]]: Documents.
        """

        collect = self.get_collection(coll_name)
        projection = dict.fromkeys(return_fields, 1) if return_fields else None

        total_cnt = await self.get_count(coll_name, filter)  # 查询总数量
        return_cnt = total_cnt if return_cnt == 'all' or 0 > return_cnt > total_cnt else return_cnt  # 返回总数量
        fetch_cnt = 0   # 已查询数量

        # _id 分页查询
        item_list, filters = [], filter
        cache_size = return_cnt if return_cnt < page_size*50 else page_size*50  # 每次查询缓存大小
        while True:

            # 最后一页更新缓存大小
            if fetch_cnt+page_size > return_cnt:
                cache_size = return_cnt-fetch_cnt

            # * _id升序 限制缓存大小 防止服务器内存暴毙
            cursor = collect.find(filters, projection).sort('_id', pymongo.ASCENDING).limit(cache_size)

            async for item in cursor:
                item_list.append(item)
                fetch_cnt += 1
                if len(item_list) == page_size:
                    yield item_list
                    page_id, item_list = item_list[-1]['_id'], []

            if item_list:
                yield item_list
                page_id = item_list[-1]['_id']

            if fetch_cnt:
                logger.info(f'mongo:{collect.full_name} | getter {fetch_cnt/return_cnt*100:>7.3f}% | _id {type(page_id)}:{str(page_id)}')
            else:
                logger.warning(f'mongo:{collect.full_name} | query null {filter}')

            if fetch_cnt == return_cnt:
                break

            # 更新查询条件
            filters = {'$and': [{'_id': {'$gt': page_id}}, filter]}

    async def rename_collection(self, old_name: str, new_name: str):
        """ Rename this collection. """
        collect = self.get_collection(old_name)
        await collect.rename(new_name)

    async def copy_collect(self, old_name: str, new_name: str):
        """ Copy the collection to the new a collection. """
        async for items in self.getter(old_name, page_size=2500):
            await self.write(new_name, items)


# ------------------------------------ Other operation ------------------------------------ #


def get_array_add_operation(_id: str, field: str, data: Sequence):
    """ Get the add array element operation """
    return UpdateOne({'_id': _id}, {'$addToSet': {field: {'$each': data}}})
