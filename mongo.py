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
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import BulkWriteError
from pymongo.operations import InsertOne, UpdateOne


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
            if password != None or username != None:
                self.client = self.get_client(host=host, port=port, username=username, password=password, authSource=database, **kwargs)
            else:
                self.client = self.get_client(host=host, port=port, **kwargs)

        self.db = self.get_database(database)

    def __str__(self) -> str:
        return f'self.db: {self.db}'

    @staticmethod
    def get_client(*args, **kwargs):
        """ Getting mongo async client. """
        return motor.motor_asyncio.AsyncIOMotorClient(*args, **kwargs)

    def get_database(self, database_name: str) -> Database:
        """ Getting database object. """
        return self.client[database_name]

    def get_collection(self, coll_name: str, **kwargs) -> Collection:
        """ Getting collection object. """
        return self.db.get_collection(coll_name, **kwargs)

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
        """ Getting collection index information. """
        return await self.get_collection(coll_name).index_information()

    async def del_collection(self, coll_name: str):
        """ Delete specified collection. """
        return await self.db.drop_collection(coll_name)

    async def write(self, coll_name: str, documents: list[dict]) -> bool:
        """Batch write documents.

        Args:
            coll_name (str): collection name.basename
            documents (list[dict]): write documents.
        Returns:
            bool: operating result.
        """

        collect = self.get_collection(coll_name)

        operate_list = []
        for item in documents:
            if item.get('_id') != None:
                operate_list.append(UpdateOne({'_id': item['_id']}, {'$set': item}, upsert=True))
            else:
                operate_list.append(InsertOne(item))

        try:
            result = await collect.bulk_write(operate_list, ordered=False)
            logger.info(f'mongo: {collect.full_name} | insert {result.inserted_count} | updata {result.upserted_count} | modified {result.matched_count} | total {len(documents)}')
            return True
        except BulkWriteError as bwe:
            logger.error(bwe.details)
            return False

    async def getter(self, coll_name: str, filter: dict = {}, return_fields: list = None, total_cnt: int = 'all', page_size: int = 500):
        """ Batch getting document builder.

        Args:
            coll_name (str): collection name.
            filter (dict, optional): filter condition. Defaults to {}.
            return_fields (list, optional): select the fields to return. Defaults to None.
            total_cnt (int, optional): getting document total quantity. Defaults to all.
            page_size (int, optional): quantity returned each time. Defaults to 500.

        Yields:
            Iterator[list[dict]]: Documents.
        """

        collect = self.get_collection(coll_name)

        projection = dict.fromkeys(return_fields, 1) if return_fields else None
        cursor = collect.find(filter, projection) if projection else collect.find(filter)

        if total_cnt == 'all':
            total_cnt = await collect.count_documents(filter)

        fetch_cnt = 0
        item_list = []
        async for item in cursor:
            item_list.append(item)

            fetch_cnt += 1
            if fetch_cnt == total_cnt:
                break

            if len(item_list) == page_size:
                logger.debug(f'mongo: {collect.full_name} | getter {fetch_cnt}/{total_cnt}')
                yield item_list
                item_list = []

        if item_list:
            logger.debug(f'mongo: {collect.full_name} | getter {fetch_cnt}/{total_cnt}')
            yield item_list

    async def rename_collection(self, old_name: str, new_name: str):
        """ Rename this collection. """
        collect = self.get_collection(old_name)
        await collect.rename(new_name)

    async def copy_collect(self, old_name: str, new_name: str):
        """ Copy the collection to the new a collection. """
        async for items in self.getter(old_name, page_size=2500):
            await self.write(new_name, items)


# ------------------------------------ Other operation ------------------------------------ #


def get_array_add_operation(_id, field: str, data: Sequence):
    """ Gets the add array element operation """
    return UpdateOne({'_id': _id}, {'$addToSet': {field: {'$each': data}}})
