import os
import sys

from dotenv import load_dotenv
import logging

load_dotenv(".env")

ZOOKEEPER_HOST = os.getenv("ZOOKEEPER_HOST")
ZOOKEEPER_PORT = int(os.getenv("ZOOKEEPER_PORT"))
PHILOSOPHERS_NUMBER = int(os.getenv("PHILOSOPHERS_NUMBER"))
START_ZOOKEEPER = os.getenv("START_ZOOKEEPER")
ZOOKEEPER_BIN_PATH = os.getenv("ZOOKEEPER_BIN_PATH")
ROOT_DIRECTORY = os.getenv("ROOT_DIRECTORY")
LOG_FILE_NAME = "philosopher.log"
LOGGER_NAME = "philosopher_logger"
LOG_FORMAT = logging.Formatter("""
============================================================================================
%(asctime)s - %(name)s - %(levelname)s - %(message)s
============================================================================================
""")

if os.path.exists(LOG_FILE_NAME):
    os.remove(LOG_FILE_NAME)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(LOGGER_NAME)

ch = logging.StreamHandler(sys.stdout)
fh = logging.FileHandler(LOG_FILE_NAME, mode="w")

ch.setLevel(logging.INFO)
ch.setFormatter(LOG_FORMAT)

fh.setLevel(logging.INFO)
fh.setFormatter(LOG_FORMAT)

logger.addHandler(ch)
logger.addHandler(fh)
