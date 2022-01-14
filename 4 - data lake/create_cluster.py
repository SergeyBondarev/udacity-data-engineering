"""
This module provides a functionality to create an Amazon EMR cluster using AWS cli.
"""

import re
import subprocess
import configparser
import logging

from logger import setup_logger


logger = logging.getLogger('create_cluster')


def read_config():
    config = configparser.ConfigParser()
    config.read('dl.cfg')
    return config


def create_cluster(auto_terminate=True):
    config = read_config()

    cluster_name = config['EMR']['CLUSTER_NAME']
    release_label = config['EMR']['RELEASE_LABEL']
    instance_count = config['EMR']['INSTANCE_COUNT']
    instance_type = config['EMR']['INSTANCE_TYPE']
    application_names = config['EMR']['APPLICATIONS'].split(',')
    key_name = config['EMR']['KEY_NAME']
    log_uri = config['EMR']['LOG_URI']
    profile = config['EMR']['PROFILE']

    application_names = ' '.join(f'Name={name.strip()}' for name in application_names)

    command = f"""aws emr create-cluster
        --name {cluster_name}
        --use-default-roles
        --release-label {release_label}
        --instance-count {instance_count}
        --instance-type {instance_type}
        --applications {application_names}
        {'--auto-terminate' if auto_terminate else ''}
        --profile {profile}
        --ec2-attributes KeyName="{key_name}"
        --log-uri {log_uri}"""

    args = re.split(r'\s+', command)

    logger.info(f'Creating EMR cluster with command: {command}')
    logger.info(f'Arguments of command line are: {args}')

    result = subprocess.run(args, shell=True)
    assert result.returncode == 0, (
        'Failed to create cluster'
    )


if __name__ == "__main__":
    setup_logger()
    logger.info("Creating cluster...")
    # create_cluster(auto_terminate=True)
