import yaml
import pprint
import boto3
import os


def get_config(printYAML=False):
    '''
    Load YAML config containing attributes of each cluster (keyed by nickname)
    '''

    config_path = os.path.dirname(os.path.abspath(__file__)) + '/config.yaml'
    with open(config_path) as config_file:
        config = yaml.load(config_file,Loader=yaml.FullLoader)

    if printYAML:
        pprint.pprint(config)

    return config


def get_region():
    config = get_config()
    region = config['params']['region']

    return region


def get_emr_client():
    '''
    Return EMR client (needed for getting info about or making changes to
    clusters). Currently configured for temp creds from STS (given role assumption).
    '''

    client = boto3.client(
        'emr',
        region_name=get_region()
    )
    return client


def get_ec2_client():
    '''
    Return EC2 client (needed for getting info about or making changes to
    clusters). Currently configured for temp creds from STS (given role assumption).
    '''

    client = boto3.client(
        'ec2',
        region_name=get_region()
    )
    return client
