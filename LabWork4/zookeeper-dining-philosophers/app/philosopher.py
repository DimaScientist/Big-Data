from multiprocessing import Process
from time import sleep

from config import ZOOKEEPER_HOST, ZOOKEEPER_PORT, PHILOSOPHERS_NUMBER, logger
from kazoo.client import KazooClient


class Philosopher(Process):

    def __init__(self,
                 root_path: str,
                 philosopher_id: int,
                 fork_path: str,
                 thinking_time: int = 5,
                 eating_time: int = 5,
                 zookeeper_host: str = f"{ZOOKEEPER_HOST}:{ZOOKEEPER_PORT}",
                 number_iteration: int = 2,
                 number_philosophers: int = PHILOSOPHERS_NUMBER):
        super().__init__()
        self.fork = fork_path
        self.root = root_path
        self.id = philosopher_id
        self.left_bound = philosopher_id
        self.right_bound = philosopher_id + 1 if philosopher_id + 1 < number_philosophers else 0,
        self.zk = KazooClient(hosts=zookeeper_host)
        self.thinking_time = thinking_time
        self.eating_time = eating_time
        self.iterations = number_iteration

    def run(self):

        self.zk.start()
        lock = self.zk.Lock(f"{self.root}/heap", self.id)
        left_fork = self.zk.Lock(f"{self.root}/{self.fork}/{self.left_bound}", self.id)
        right_fork = self.zk.Lock(f"{self.root}/{self.fork}/{self.right_bound}", self.id)

        i = 0
        logger.info(f"Philosopher {self.id} has came")
        while i < self.iterations:

            logger.info(f"Philosopher {self.id} is thinking")
            sleep(self.thinking_time)
            with lock:
                if len(left_fork.contenders()) == 0 \
                        and len(right_fork.contenders()) == 0:
                    left_fork.acquire()
                    right_fork.acquire()

                if left_fork.is_acquired:
                    logger.info(f"Philosopher {self.id} is eating")
                    sleep(self.eating_time)
                    left_fork.release()
                    right_fork.release()
            i += 1

        logger.info(f"Philosopher {self.id} has gone")
        self.zk.stop()
        self.zk.close()
