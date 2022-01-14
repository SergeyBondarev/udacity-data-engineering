"""
This module provides logger utilities.
"""

import logging
import logging.config
import yaml


def setup_logger():
    with open('logging.yaml', 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f.read())
        logging.config.dictConfig(config)