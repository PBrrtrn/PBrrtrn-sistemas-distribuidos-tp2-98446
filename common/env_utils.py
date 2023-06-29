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


def parse_queue_bindings_with_client_id(queue_bindings: str, client_id):
    split_bindings = queue_bindings.split(':')
    result = {split_bindings[0]: [split_bindings[1] + client_id]}
    return result


def parse_node_id_to_container_name_mapping(mapping: str):
    result = {}

    items = mapping.split(',')
    for item in items:
        split_item = item.split(':')
        result[int(split_item[0])] = split_item[1]

    return result
