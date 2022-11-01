import random
from cassandra.cluster import Cluster
from cassandra.policies import RoundRobinPolicy
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement
from src.helpers import const

from locust import User, events
from dotenv import dotenv_values

import os
import time


@events.init_command_line_parser.add_listener
def _(parser):
    parser.add_argument(
        "--replication-sync",
        type=str,
        env_var="REPLICATION_SYNC",
        default="ONE",
        help="Replication sync",
    )


class CassandraClient(object):

    def __init__(self, host, replication_sync):
        self.config = dotenv_values("./config/.env")
        self.replication_sync = self._get_replication_sync(replication_sync)
        cluster = Cluster([self.config.get("CASSANDRA_HOST")], port=9042)
        self.session = cluster.connect(self.config.get("DATABASE_NAME"))
        self.session.default_timeout = 15
        self.database_name = self.config.get("DATABASE_NAME")
        self.collection_name = self.config.get("COLLECTION_NAME")
        self.data_limit = self.config.get("DATA_LIMIT")
        
        self.insert_data = self._get_insert_data()

    def select(self):
        name = "Cassandra SELECT query"

        start_time = time.time()
        try:
            query = SimpleStatement(
                f"SELECT * FROM {self.database_name}.{self.collection_name} LIMIT {self.data_limit}",
                consistency_level=self.replication_sync
            )
            [a for a in self.session.execute(query)]
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
    
    def insert(self):
        name = "Cassandra INSERT query"

        start_time = time.time()
        try:
            key = random.randint(0,100000)
            columns= ["id"]
            values = [str(key)]

            columns.extend(self.insert_data["columns"])
            values.extend(self.insert_data["values"])

            query = SimpleStatement(
                f"INSERT INTO {self.database_name}.{self.collection_name} ({','.join(columns)}) VALUES ({','.join(values)})",
                consistency_level=self.replication_sync,
            )
            self.session.execute(query)
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
            return ConsistencyLevel.ALL
        else:
            return ConsistencyLevel.ONE
    
    def _get_insert_data(self):
        return {"columns": [str(key) for key in const.DATA.keys()], "values": [str(value) for value in const.DATA.values()]}


class CassandraLocust(User):
    abstract = True

    def __init__(self, *args, **kwargs):
        super(CassandraLocust, self).__init__(*args, **kwargs)
        self.client = CassandraClient(
            self.host, self.environment.parsed_options.replication_sync
        )
