import collections.abc
import os.path
from typing import MutableMapping

import yaml
from jsonschema import validate

from kl_api_common.env import env

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

    path_config_base = env.str("PATH_CONFIG_BASE", "config.yaml")
    path_config_override = env.str("PATH_CONFIG_OVERRIDE", "config-override.yaml")
    path_config_schema = env.str("PATH_CONFIG_SCHEMA", "config.schema.json")

    # Load default config
    with open(path_config_base, "r", encoding="utf-8") as config_file:
        config = yaml.safe_load(config_file)

    # Load overriding config
    if os.path.exists(path_config_override):
        with open(path_config_override, "r", encoding="utf-8") as config_file:
            config = merge_dict(config, yaml.safe_load(config_file))

    # Validate config
    with open(path_config_schema, "r") as config_schema:
        validate(config, yaml.safe_load(config_schema))

    _config = config

    return _config
