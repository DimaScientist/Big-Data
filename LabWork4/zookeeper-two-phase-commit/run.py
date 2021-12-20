import subprocess

from kazoo.client import KazooClient

from app import Coordinator, Client
from config import START_ZOOKEEPER, ZOOKEEPER_BIN_PATH, ROOT_DIRECTORY, CLIENT_NUMBER, ZOOKEEPER_HOST, ZOOKEEPER_PORT

if __name__ == '__main__':
    if START_ZOOKEEPER.lower() == "yes":
        subprocess.run(f"{ZOOKEEPER_BIN_PATH}/zkServer.sh start", shell=True)

    zk = KazooClient(hosts=f"{ZOOKEEPER_HOST}:{ZOOKEEPER_PORT}")
    zk.start()

    root_path = f"/{ROOT_DIRECTORY}"
    transaction_path = f"{root_path}/tx"

    if zk.exists(root_path):
            zk.delete(root_path, recursive=True)

    zk.create(root_path)
    zk.create(transaction_path)

    coordinator = Coordinator(zookeeper_client=zk, root_path=root_path, transaction_dir=transaction_path)
    coordinator.run()

    for i in range(CLIENT_NUMBER):
        client = Client(root_dir=transaction_path, client_id=(i+1))
        client.start()
