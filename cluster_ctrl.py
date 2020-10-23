import sys
import time
import json
import boto3
import configparser

def get_vpc_security_group_ids(session, groupNames):
    client = session.client('ec2')
    resp = client.describe_security_groups()
    return [ g['GroupId'] for g in resp['SecurityGroups']
                if g['GroupName'] in groupNames ]

def cluster_kickoff(session, clusterId, snapshotId,
    securityGroups, iamRoles):
    security_ids = get_vpc_security_group_ids(session,
        securityGroups)
    client = session.client('redshift')
    resp = client.restore_from_cluster_snapshot(
        ClusterIdentifier=clusterId,
        SnapshotIdentifier=snapshotId,
        PubliclyAccessible=True,
        VpcSecurityGroupIds=security_ids,
        IamRoles=iamRoles)
    print('{}: {}'.format(resp['Cluster']['ClusterIdentifier'],
        resp['Cluster']['ClusterStatus']))
    return client

def cluster_shutdown(session, clusterId):
    client = session.client('redshift')
    resp = client.delete_cluster(
        ClusterIdentifier=clusterId,
        SkipFinalClusterSnapshot=True)
    print('{}: {}'.format(resp['Cluster']['ClusterIdentifier'],
        resp['Cluster']['ClusterStatus']))
    return client

def get_cluster(client, config):
    resp = client.describe_clusters()
    clusters = [ c for c in resp['Clusters']
        if c['ClusterIdentifier'] == config['AWS']['CLUSTER_ID'] ]
    assert len(clusters) == 1
    return clusters[0]

def cluster_is_creating(cluster):
    return cluster['ClusterStatus'] == 'creating'

def sleep_and_print(sleepTime):
    print('Cluster creating...')
    for i in range(sleepTime,0,-1):
        sys.stdout.write(f'\rChecking cluster status in: {i:2d}')
        sys.stdout.flush()
        time.sleep(1)

def update_airflow_configuration(client, config):
    cluster = get_cluster(client, config)
    while cluster_is_creating(cluster):
        sleep_and_print(20)
        cluster = get_cluster(client, config)
    print('Cluster available')
    config['AF_CONN_REDSHIFT']['HOST'] = cluster['Endpoint']['Address']
    config['AF_CONN_REDSHIFT']['PORT'] = str(
        cluster['Endpoint']['Port'])
    return config


if __name__ == '__main__':
    try:
        action = sys.argv[1]
    except:
        print('Declare an action to take: "start" or "stop"')

    try:
        config_file = sys.argv[2]
    except:
        print('Configuration file required')

    cfg = configparser.ConfigParser()
    cfg.read(config_file)
    aws_cfg = cfg['AWS']
    cluster_id = aws_cfg['CLUSTER_ID']
    
    session = boto3.Session(
        aws_access_key_id=aws_cfg['ACCESS_KEY'],
        aws_secret_access_key=aws_cfg['SECRET_KEY'],
        region_name=aws_cfg['REGION']
    )
    if action == 'start':
        client = cluster_kickoff(session, cluster_id,
            aws_cfg['SNAPSHOT_ID'],
            aws_cfg['VPC_SECURITY_GROUPS'].split(','),
            aws_cfg['IAM_ROLES'].split(',')
        )
        print('Cluster activated')
        cfg = update_airflow_configuration(client, cfg)
        with open(config_file,'w') as f:
            cfg.write(f)
        print('Configuration file updated')

    elif action == 'stop':
        client = cluster_shutdown(session, cluster_id)
        print('Cluster deactivated')

    elif action == 'test':
        sleep_and_print(10)
        sleep_and_print(5)