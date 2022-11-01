import random
from dotenv import dotenv_values
from locust import User, events
from pymongo import MongoClient
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern
from src.helpers import const
import os
import time
import copy

@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--replication-sync",
        type=str,
        env_var="REPLICATION_SYNC",
        default="ONE",
        help="Replication sync",
    )


class MongoDBClient(object):
    def __init__(self, replication_sync):
        self.config = dotenv_values("./config/.env")
        self.database_name = self.config.get("DATABASE_NAME")
        self.collection_name = self.config.get("COLLECTION_NAME")
        self.data_limit = int(self.config.get("DATA_LIMIT"))

        read_replication_sync, write_replication_sync = self._get_replication_sync(replication_sync)
        self.write_concern = WriteConcern(w=write_replication_sync, j=True)
        self.read_concern = ReadConcern(read_replication_sync)

        self.session = MongoClient(host=self.config.get("MONGODB_HOST"), connect=False)
        

    def select(self, name=None):
        name = "MongoDB SELECT query"

        start_time = time.time()
        try:
            database = self.session[self.database_name]
            collection = database.get_collection(self.collection_name, read_concern=self.read_concern)
            [a for a in collection.find().limit(self.data_limit)]
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request_failure.fire(
                request_type="query",
                name=name,
                response_time=total_time,
                exception=e,
                response_length=0,
            )
        else:
            total_time = int((time.time() - start_time) * 1000)
            events.request_success.fire(
                request_type="query",
                name=name,
                response_time=total_time,
                response_length=0,
            )
    
    def insert(self, name=None):
        name = "MongoDB INSERT query"

        key = int(time.time()) + random.randint(0,1000000000)
        data_to_insert = copy.deepcopy(const.DATA)
        data_to_insert.update({"id": key})

        start_time = time.time()
        try:
            database = self.session[self.database_name]
            collection = database.get_collection(self.collection_name, write_concern=self.write_concern)

            collection.insert_one(data_to_insert)
        except Exception as e:
            total_time = int((time.time() - start_time) * 1000)
            events.request_failure.fire(
                request_type="query",
                name=name,
                response_time=total_time,
                exception=e,
                response_length=0,
            )
        else:
            total_time = int((time.time() - start_time) * 1000)
            events.request_success.fire(
                request_type="query",
                name=name,
                response_time=total_time,
                response_length=0,
            )
    
    def _get_replication_sync(self, replication_sync):
        if replication_sync == "STRONG":
            return "linearizable", 3
        else:
            return "local", 1

class MongoDBLocust(User):
    abstract = True

    def __init__(self, *args, **kwargs):
        super(MongoDBLocust, self).__init__(*args, **kwargs)
        self.client = MongoDBClient(
            self.environment.parsed_options.replication_sync
        )
