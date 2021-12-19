import logging
import subprocess

from kazoo.client import KazooClient

from app.philosopher import Philosopher
from config import START_ZOOKEEPER, ZOOKEEPER_BIN_PATH, \
    PHILOSOPHERS_NUMBER, ZOOKEEPER_HOST, ZOOKEEPER_PORT, ROOT_DIRECTORY

if __name__ == '__main__':
    if START_ZOOKEEPER.lower() == "yes":
        subprocess.run(f"{ZOOKEEPER_BIN_PATH}/zkServer.sh start", shell=True)

    master = KazooClient(hosts=f"{ZOOKEEPER_HOST}:{ZOOKEEPER_PORT}")
    master.start()

    if master.exists(f"/{ROOT_DIRECTORY}"):
        master.delete(f"/{ROOT_DIRECTORY}", recursive=True)

    master.create(f"/{ROOT_DIRECTORY}")
    master.create(f"/{ROOT_DIRECTORY}/heap")
    master.create(f"/{ROOT_DIRECTORY}/fork")

    for i in range(PHILOSOPHERS_NUMBER):
        master.create(f"/{ROOT_DIRECTORY}/fork/{str(i + 1)}")

    for i in range(PHILOSOPHERS_NUMBER):
        philosopher = Philosopher(
            root_path=f"/{ROOT_DIRECTORY}",
            philosopher_id=(i + 1),
            fork_path="fork"
        )
        philosopher.start()
