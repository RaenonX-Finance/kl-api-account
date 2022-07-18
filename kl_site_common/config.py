import collections.abc
from typing import MutableMapping

import yaml
from jsonschema import validate

_config: MutableMapping = {}


def merge_dict(map_1: MutableMapping, map_2: MutableMapping) -> MutableMapping:
    # Modified from https://stackoverflow.com/a/3233356/11571888
    for k, v in map_2.items():
        if isinstance(v, collections.abc.MutableMapping):
            map_1[k] = merge_dict(map_1.get(k, {}), v)
        else:
            map_1[k] = v

    return map_1


def get_config() -> MutableMapping:
    global _config

    if _config:
        return _config

    # Load default config
    with open("config.yaml", "r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    # Load overriding config
    with open("config-override.yaml", "r", encoding="utf-8") as config_file:
        config = merge_dict(config, yaml.safe_load(config_file))

    # Validate config
    with open("config.schema.json", "r") as config_schema:
        validate(config, yaml.safe_load(config_schema))

    _config = config

    return _config
