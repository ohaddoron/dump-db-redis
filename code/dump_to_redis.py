import asyncio
import json
from functools import lru_cache
from uuid import uuid4

import aioredis
import motor.motor_asyncio
import numpy as np
import pymongo
import pymongo.database
import motor.motor_asyncio

import typer

from typer import Typer
from loguru import logger
import typing as tp
from mongoengine import connect

app = Typer()


@lru_cache(maxsize=128)
def init_cached_database(connection_string: str, db_name: str,
                         alias: tp.Optional[str] = None, async_flag=False) -> tp.Union[
    pymongo.database.Database, motor.motor_asyncio.AsyncIOMotorDatabase]:
    """
    initializes a cahced handle to the mongodb database

    :param connection_string: mongodb connection string
    :type connection_string: str
    :param db_name: name of the database to connect to
    :type db_name: str
    :return: database handle
    :rtype: :class:`pymongo.database.Database`
    """
    alias = alias or uuid4().hex
    if not async_flag:
        return connect(host=connection_string, alias=alias)[db_name]
    else:
        return motor.motor_asyncio.AsyncIOMotorClient(host=connection_string)[db_name]


async def aggregate_attributes(connection_string: str, db_name: str, collection: str, sample: str) -> dict:
    db: motor.motor_asyncio.AsyncIOMotorDatabase = init_cached_database(
        connection_string=connection_string,
        db_name=db_name,
        async_flag=True
    )
    cursor = db[collection].aggregate([
        {
            '$match': {
                'sample': sample
            }
        }, {
            '$group': {
                '_id': '$sample',
                'data': {
                    '$push': {
                        'k': '$name',
                        'v': '$value'
                    }
                },
                'patient': {
                    '$push': '$patient'
                }
            }
        }, {
            '$project': {
                'user': {
                    '$arrayElemAt': [
                        '$patient', 0
                    ]
                },
                'sample': '$_id',
                '_id': 0,
                'data': {
                    '$arrayToObject': '$data'
                }
            }
        }, {
            '$replaceRoot': {
                'newRoot': {
                    '$mergeObjects': [
                        '$$ROOT', '$data'
                    ]
                }
            }
        }, {
            '$project': {
                'data': 0,
                'user': 0,
                'sample': 0
            }
        }
    ])
    return (await cursor.to_list(None))[0]


async def patients(mongo_uri: str, db_name: str, collection: str):
    db = init_cached_database(connection_string=mongo_uri, db_name=db_name, async_flag=True)
    patients = await db[collection].distinct('patient')
    for patient in patients:
        yield patient


async def main(redis_host: str, redis_port: int, mongo_uri: str, db_name: str, collection: str):
    redis = aioredis.from_url(f'redis://{redis_host}:{redis_port}')
    db = init_cached_database(connection_string=mongo_uri, db_name=db_name, async_flag=True)

    async for patient in patients(mongo_uri=mongo_uri, db_name=db_name, collection=collection):
        samples = await db[collection].find(dict(patient=patient)).distinct('sample')
        for sample in samples:
            attributes = await aggregate_attributes(connection_string=mongo_uri,
                                                    db_name=db_name,
                                                    collection=collection,
                                                    sample=sample)
            await redis.set(f'{sample}-{collection}',
                            value=json.dumps(attributes))

        logger.debug(f'Patient: {patient} completed')


@app.command()
def run(redis_host: str = typer.Option(default='localhost'), redis_port: int = typer.Option(default=6379),
        mongo_uri: str = typer.Option(...),
        db_name: str = typer.Option(...),
        collection: str = typer.Option(...)
        ):
    asyncio.run(
        main(redis_host=redis_host, redis_port=redis_port, mongo_uri=mongo_uri, db_name=db_name, collection=collection))


if __name__ == '__main__':
    app()
