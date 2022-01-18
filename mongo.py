#!/usr/bin/env python3
# -*- encoding: utf-8 -*-
'''
@File   :   mongo_async.py
@Time   :   2021/12/21 16:19
@Author :   Blank
@Version:   2.1
@Desc   :   Mongo pcblib
'''

from typing import Sequence

import motor.motor_asyncio
import pymongo
from loguru import logger
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError
from pymongo.operations import DeleteOne, InsertOne, UpdateOne
from pymongo.results import BulkWriteResult


# ------------------------------------ CRUD ------------------------------------ #

# TODO 分离函数功能


# ------------------------------------  ------------------------------------ #


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

    # ------------------------------------  ------------------------------------ #

    async def create_index(self, coll_name: str, keys: Sequence, sort_type=pymongo.ASCENDING, unique=False):
        """Creates an index on this collection.

        Args:
            coll_name ([type]): collection name.
            keys (Sequence): The key that creates the index(Single or compound).
            sort_type (int, optional): Type of index created. Defaults to pymongo.ASCENDING.
                `pymongo.ASCENDING`, `pymongo.DESCENDING`, `pymongo.GEO2D`, `pymongo.GEOHAYSTACK`, `pymongo.GEOSPHERE`, `pymongo.HASHED`, `pymongo.TEXT`
            unique (bool, optional): creates a uniqueness constraint on the index. Defaults to True.

        """
        coll = self.get_collection(coll_name)
        _keys = [(key, sort_type) for key in keys] if isinstance(keys, (list, tuple)) else [(keys, sort_type)]

        return await coll.create_index(_keys, unique=unique, background=True)

    async def get_index_info(self, coll_name: str):
        """ Get collection index information. """
        return await self.get_collection(coll_name).index_information()

    async def get_count(self, coll_name: str, filter: dict = {}):
        """ Get the number of collection documents. """
        collect = self.get_collection(coll_name)
        return await collect.count_documents(filter) if filter else await collect.estimated_document_count()

    # ------------------------------------ CRUD ------------------------------------ #

    async def write(self, coll_name: str, documents: list[dict], log_switch: bool = True, collect: Collection = None) -> bool:

        """Batch write documents.

        Args:
            coll_name (str): collection name.
            documents (list[dict]): write documents.
            log_switch (bool, optional): info level log switch. Defaults to True.
            collect (Collection, optional): Specifying collection links. Defaults to None.

        Returns:
            bool: operating result.
        """
        if not documents:
            logger.warning('documents is null')
            return True

        # 有_id则更新数据 无则插入
        operate_list = [UpdateOne({'_id': item['_id']}, {'$set': item}, upsert=True) if item.get('_id') != None else InsertOne(item) for item in documents]

        try:
            collect = collect or self.get_collection(coll_name)
            result: BulkWriteResult = await collect.bulk_write(operate_list, ordered=False)
            log_switch and logger.info(f'mongo:{collect.full_name} | insert {result.inserted_count} | updata {result.upserted_count} | modified {result.matched_count} | total {len(documents)}')
            return True
        except BulkWriteError as e:
            logger.error(e.details)
            return False

    async def delete(self, coll_name: str, documents: list[dict], log_switch: bool = True) -> bool:
        """Delete mongo document data.

        Args:
            coll_name (str):  collection name.
            documents (list[dict]): delect documents.
            log_switch (bool, optional): info level log switch. Defaults to True.

        Returns:
            bool: operating result.
        """

        # 根据_id删除的数据
        operate_list = [DeleteOne({'_id': i['_id']}) for i in documents if i.get('_id')]

        if not operate_list:
            logger.warning('documents no _id')
            return True

        try:
            collect = self.get_collection(coll_name)
            result: BulkWriteResult = await collect.bulk_write(operate_list, ordered=False)
            log_switch and logger.info(f'mongo:{collect.full_name} | deleted {result.deleted_count} | total {len(documents)}')
            return True
        except BulkWriteError as e:
            logger.error(e.details)
            return False

    async def getter(self, coll_name: str, filter: dict = {}, return_fields: list = None,
                     return_cnt: int = 'all', page_size: int = 500, sort_query: bool = True,
                     log_switch: bool = True):
        """ Batch get document generator.

        Args:
            coll_name (str): collection name.
            filter (dict, optional): filter condition. Defaults to {}.
            return_fields (list, optional): select the fields to return. Defaults to None.
            return_cnt (int, optional): getting document total quantity. Defaults to all.
            page_size (int, optional): quantity returned each time. Defaults to 500.
            sort_query (bool, optional): _id sort query. Defaults to True.
            log_switch (bool, optional): info level log switch. Defaults to True.

        Yields:
            Iterator[list[dict]]: Documents.
        """

        collect = self.get_collection(coll_name)

        if (total_cnt := await self.get_count(coll_name, filter)) == 0:  # 查询数量
            logger.warning(f'mongo:{collect.full_name} | query null | filter {filter}')
            return

        return_cnt = total_cnt if return_cnt == 'all' or 0 > return_cnt > total_cnt else return_cnt  # 返回数量

        fetch_cnt, item_list, filters = 0, [], filter
        projection = dict.fromkeys(return_fields, 1) if return_fields else None  # 返回字段
        cache_size = return_cnt if return_cnt < page_size*50 else page_size*50 if sort_query else return_cnt  # 查询大小
        while True:

            # 最后一页时 更新缓存大小
            if fetch_cnt+page_size > return_cnt:
                cache_size = return_cnt-fetch_cnt

            cursor = collect.find(filters, projection).limit(cache_size).batch_size(page_size)
            sort_query and cursor.sort('_id', pymongo.ASCENDING)

            async for item in cursor:
                item_list.append(item)
                fetch_cnt += 1
                if len(item_list) == page_size:
                    page_id = item_list[-1]['_id']
                    sort_query and log_switch and logger.info(f'mongo:{collect.full_name} | get {fetch_cnt/return_cnt*100:>7.2f}% | fetch {fetch_cnt}')
                    yield item_list
                    item_list = []
            if item_list:
                yield item_list

            log_switch and logger.info(f'mongo:{collect.full_name} | get {fetch_cnt/return_cnt*100:>7.2f}% | fetch {fetch_cnt} | end \'_id\' {type(page_id)}:{str(page_id)}')

            if fetch_cnt >= return_cnt:
                break

            # 更新查询条件
            filters = {'$and': [{'_id': {'$gt': page_id}}, filter]}

    async def find_one(self, coll_name: str, filter: dict = {}, return_fields: list = None):
        """Get a single document from the database.

        Args:
            coll_name (str): collection name.
            filter (dict, optional): filter condition. Defaults to {}.
            return_fields (list, optional): select the fields to return. Defaults to None.

        Returns:
            dict: document or {}
        """

        projection = dict.fromkeys(return_fields, 1) if return_fields else None
        return await self.get_collection(coll_name).find_one(filter, projection) or {}

    # ------------------------------------ collection ------------------------------------ #

    async def get_collection_list(self) -> list:
        """ Get collection names list"""
        return await self.db.list_collection_names()

    async def del_collection(self, coll_name: str):
        """ Delete specified collection. """
        return await self.db.drop_collection(coll_name)

    async def rename_collection(self, old_name: str, new_name: str):
        """ Rename this collection. """
        # TODO admin权限判断处理
        await self.get_collection(old_name).rename(new_name)

    async def copy_collection(self, old_name: str, new_name: str):
        """ Copy the collection to the new a collection. """

        async for items in self.getter(old_name, page_size=5000):
            await self.write(coll_name=new_name, documents=items, log_switch=False)
        logger.info(f'{old_name} copy to {new_name} done!')


# ------------------------------------ Other operation ------------------------------------ #


def get_array_add_operation(_id: str, field: str, data: Sequence):
    """ Get the add array element operation """
    return UpdateOne({'_id': _id}, {'$addToSet': {field: {'$each': data}}})
