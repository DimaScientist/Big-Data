from random import randint
from threading import Thread
from time import sleep

from kazoo.client import KazooClient

from config import ZOOKEEPER_PORT, ZOOKEEPER_HOST, logger


class Client(Thread):

    def __init__(self,
                 root_dir: str,
                 client_id: int,
                 waiting_time: int = 10,
                 zookeeper_host: str = f"{ZOOKEEPER_HOST}:{ZOOKEEPER_PORT}"):
        super().__init__()
        self.dir = f"{root_dir}/node_{client_id}"
        self.root_path = root_dir
        self.id = client_id
        self.zk = KazooClient(hosts=zookeeper_host)
        self.waiting_time = waiting_time

    def run(self) -> None:
        self.zk.start()

        activity = b"commit" if randint(0, 10) > 5 else b"rollback"
        logger.info(f"{str(activity.decode('utf-8'))} from client {self.id}")
        self.zk.create(path=self.dir, value=activity, ephemeral=True)

        @self.zk.DataWatch(self.dir)
        def watch_node(data, stat):
            if stat.version > 0:
                logger.info(f"{data.decode('utf-8')} from client {self.id} has done")

        sleep(self.waiting_time)

        self.zk.stop()
        self.zk.close()


