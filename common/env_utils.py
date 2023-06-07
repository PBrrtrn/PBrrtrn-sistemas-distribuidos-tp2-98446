import os
from configparser import ConfigParser


def read_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    return config["DEFAULT"]


def parse_queue_bindings(queue_bindings: str):
    split_bindings = queue_bindings.split(':')
    result = {split_bindings[0]: split_bindings[1].split(',')}
    return result
