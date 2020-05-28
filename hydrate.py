#!/usr/bin/env python3

#
# This script will walk through all the tweet id files and
# hydrate them with twarc. The line oriented JSON files will
# be placed right next to each tweet id file.
#
# Note: you will need to install twarc, tqdm, and run twarc configure
# from the command line to tell it your Twitter API keys.
#

import gzip
import json
from pathlib import Path

from pymongo import MongoClient

from tqdm import tqdm
from twarc import Twarc
from loguru import logger

LOGGER_ROOT = "./logs/"
logger.add(LOGGER_ROOT + "general.log", level="DEBUG", rotation="5 MB")

twarc = Twarc()
data_dirs = ["2020-01", "2020-02", "2020-03", "2020-04", "2020-05"]


@logger.catch()
def main():
    # TODO Handle duplicate directories
    client = mongo_connect("tweets", "covid19Usc")
    for data_dir in data_dirs:
        for path in Path(data_dir).iterdir():
            if path.name.endswith(".txt"):
                hydrate(path, client, data_dir)


def _reader_generator(reader):
    b = reader(1024 * 1024)
    while b:
        yield b
        b = reader(1024 * 1024)


def raw_newline_count(fname):
    """
    Counts number of lines in file
    """
    f = open(fname, "rb")
    f_gen = _reader_generator(f.raw.read)
    return sum(buf.count(b"\n") for buf in f_gen)


def mongo_connect(db, collection, host="localhost", port=27017):
    client = MongoClient(host, port)
    db = client[db]
    collection = db[collection]

    return collection


def verif_log_file_exist(data_dir):
    root = Path(data_dir)
    log_file = root / Path(data_dir + ".log")
    if not log_file.is_file():
        logger.info("Creating log file for the repository")
        log_file.touch()
    return log_file


def is_already_processed(log_file, id_file):
    with log_file.open(mode="r+") as f:
        for line in f:
            if str(id_file) in line:
                return True
        else:
            f.write(str(id_file))
            return False


@logger.catch()
def hydrate(id_file, mongo_client, data_dir):
    print("hydrating {}".format(id_file))

    log_file = verif_log_file_exist(data_dir)

    if is_already_processed(log_file, id_file):
        logger.info("File already processed, moving to the next")
        return

    num_ids = raw_newline_count(id_file)

    with tqdm(total=num_ids) as pbar:
        for tweet in twarc.hydrate(id_file.open()):
            mongo_client.insert_one(tweet)
            pbar.update(1)


if __name__ == "__main__":
    main()
