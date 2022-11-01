from dotenv import dotenv_values
from locust import User, events
from src.helpers import const
import os
import random
import redis
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


class RedisClient(object):
    def __init__(self, host, replication_sync):
        self.replication_sync = self._get_replication_sync(replication_sync)
        self.session = redis.from_url(self.config["REDIS_HOST"], password=self.config["REDIS_PWD"])
        self.config = dotenv_values("./config/.env")
        self.database_name = self.config.get("DATABASE_NAME")
        self.collection_name = self.config.get("COLLECTION_NAME")
        

    def select(self, name=None):
        name = "Redis SELECT query"

        start_time = time.time()
        try:
            self.session.hgetall(self.collection_name)
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
        name = "Redis INSERT query"

        start_time = time.time()
        try:
            key = random.randint(0,1000000)
            self.session.hmset(name=f"{self.collection_name}:{key}", mapping=const.DATA)
            self.session.wait(self.replication_sync)
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
            return 3
        else:
            return 1

class RedisLocust(User):
    abstract = True

    def __init__(self, *args, **kwargs):
        super(RedisClient, self).__init__(*args, **kwargs)
        self.client = RedisClient(
            self.host, self.environment.parsed_options.replication_sync
        )
