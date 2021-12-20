from threading import Timer

from kazoo.client import KazooClient

from config import ZOOKEEPER_HOST, ZOOKEEPER_PORT, CLIENT_NUMBER, ROOT_DIRECTORY, logger


class Coordinator:

    def __init__(self, zookeeper_client: KazooClient,
                 timeout: int = 3,
                 client_number: int = CLIENT_NUMBER,
                 root_path: str = ROOT_DIRECTORY,
                 transaction_dir: str = f"/{ROOT_DIRECTORY}/tx"):
        self.zk = zookeeper_client
        self.root_path = f"/{root_path}"
        self.transaction_path = transaction_dir
        self.client_number = client_number
        self.timeout = timeout
        self.timer: Timer = None

    def run(self):

        transaction_dir = f"{self.root_path}/tx"
        zk_coordinator = self.zk
        if zk_coordinator.connected is False:
            zk_coordinator.start()

        def voting():
            children = zk_coordinator.get_children(transaction_dir)
            commit_transactions = 0
            rollback_transactions = 0
            if len(children) > 0:
                for child in children:
                    node_info = zk_coordinator.get(f"{transaction_dir}/{child}")
                    if node_info[0] == b'commit':
                        commit_transactions += 1
                    else:
                        rollback_transactions += 1
                target_action = b"commit" if commit_transactions > rollback_transactions else b"rollback"
                for client in children:
                    zk_coordinator.set(f"{transaction_dir}/{client}", target_action)

        @zk_coordinator.ChildrenWatch(transaction_dir)
        def children_watch(children):
            if self.timer is not None:
                self.timer.cancel()

            self.timer = Timer(interval=self.timeout, function=voting)
            self.timer.start()

            if len(children) < self.client_number:
                logger.info("Synchronizing all clients")
            elif len(children) == self.client_number:
                logger.info("Processing actions")
                self.timer.cancel()
                voting()
