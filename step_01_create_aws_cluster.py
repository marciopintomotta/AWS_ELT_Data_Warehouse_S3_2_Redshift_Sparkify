import boto3
import configparser
from botocore.exceptions import ClientError
import json
import time


config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                = config.get('AWS','KEY')
SECRET             = config.get('AWS','SECRET')

DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")

DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")

DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")


def create_aws_clients_resources():

    """
        create aws clients & resources
    
    """

    ec2 = boto3.resource('ec2',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                        )
    
    print("ec2 client created")

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )
    
    print("iam client created")

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    
    print("redshift client created")

    return  ec2, iam, redshift


def create_aws_iam_role(iam):

    """
        create aws iam role and attaching policy
    
    """
    
    try:
        print("Creating a new IAM Role")  
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )    
    except Exception as e:
        print(e)
        
    print('Attaching policy created')
    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                        )['ResponseMetadata']['HTTPStatusCode']
    
    print("Get the DWH IAM ROLE created in AWS")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    return roleArn


def create_aws_cluster(ec2, iam, redshift):

    """
        create aws redshift cluster
    
    """
   
    try:
        roleArn = create_aws_iam_role(iam)
        response = redshift.create_cluster(        
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )

    except Exception as e:
        print(e)

    print("the cluster is being created... please wait!")
    while True:
        response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            print("The cluster has been created and is now available for use!!")
            break
        time.sleep(10)

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    print('')
    print(" ---------------------  Update dwh.cfg  ---------------------------")
    print(" --- don't forget to update the config file with this information --")
    print("DWH_ENDPOINT:  {}".format(myClusterProps['Endpoint']['Address']))
    print("DWH_ROLE_ARN:  {}".format(myClusterProps['IamRoles'][0]['IamRoleArn']))
    print(" -------------------------------------------------------------------")
    print('')

def create_aws_cluster_direct():

    redshift = boto3.client('redshift',
                       region_name="us-west-2",
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                       )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    roleArn = iam.get_role(RoleName='dwhRole')['Role']['Arn']

    try:
        response = redshift.create_cluster(      


            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            #Roles (for s3 access)
            IamRoles=[roleArn]  
        )
    except Exception as e:
        print(e)

    print("the cluster is being created... please wait!")
    while True:
        response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
        cluster_info = response['Clusters'][0]
        if cluster_info['ClusterStatus'] == 'available':
            print("The cluster has been created and is now available for use!!")
            break
        time.sleep(10)

    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    
    print('')
    print(" ---------------------  Update dwh.cfg  ---------------------------")
    print(" --- don't forget to update the config file with this information --")
    print("DWH_ENDPOINT:  {}".format(myClusterProps['Endpoint']['Address']))
    print("DWH_ROLE_ARN:  {}".format(myClusterProps['IamRoles'][0]['IamRoleArn']))
    print(" -------------------------------------------------------------------")
    print('')


def main():

    ec2, iam, redshift = create_aws_clients_resources()
    create_aws_cluster(ec2, iam, redshift)


if __name__ == "__main__":
    main()