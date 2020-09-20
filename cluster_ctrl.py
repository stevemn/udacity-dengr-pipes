import sys
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
    return client.restore_from_cluster_snapshot(
        ClusterIdentifier=clusterId,
        SnapshotIdentifier=snapshotId,
        PubliclyAccessible=True,
        VpcSecurityGroupIds=security_ids,
        IamRoles=iamRoles)

def cluster_shutdown(session, clusterId):
    client = session.client('redshift')
    return client.delete_cluster(
        ClusterIdentifier=clusterId,
        SkipFinalClusterSnapshot=True)


if __name__ == '__main__':
    try:
        action = sys.argv[1]
    except:
        print('Declare an action to take: "start" or "stop"')

    cfg = configparser.ConfigParser()
    cfg.read('pipeline.ini')
    aws_cfg = cfg['AWS']
    cluster_id = aws_cfg['CLUSTER_ID']
    
    session = boto3.Session(
        aws_access_key_id=aws_cfg['ACCESS_KEY'],
        aws_secret_access_key=aws_cfg['SECRET_KEY'],
        region_name=aws_cfg['REGION']
    )
    if action == 'start':
        resp = cluster_kickoff(session, cluster_id,
            aws_cfg['SNAPSHOT_ID'],
            aws_cfg['VPC_SECURITY_GROUPS'].split(','),
            aws_cfg['IAM_ROLES'].split(',')
        )
        # aws_data = json.loads(resp)
        # aws_data['Cluster']['ClusterIdentifier']
    elif action == 'stop':
        resp = cluster_shutdown(session, cluster_id)
    
    if resp:
        print(f'AWS response:\n\t{resp}')