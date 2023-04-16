import boto3
import configparser
from botocore.exceptions import ClientError
import json
import time

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                = config.get('AWS','KEY')
SECRET             = config.get('AWS','SECRET')

DWH_CLUSTER_IDENTIFIER  = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_IAM_ROLE_NAME       = config.get('DWH', "DWH_IAM_ROLE_NAME")


def create_connection_aws_cluster():
    
    """
    
        create a connection to existing cluster
    
    """

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )
    
    return redshift, iam


def delete_aws_iam_role(iam):
    
    """
        delete aws iam role

    """
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)


def delete_aws_cluster(redshift):
    
    """
        delete aws cluster
    
    """

    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
    print("initiating the cluster deletion process: {}".format(DWH_CLUSTER_IDENTIFIER))

    while True:
        try:
            response = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)
            cluster_info = response['Clusters'][0]
            time.sleep(10)
        except:
            print("The the cluster was deleted!")
            break


def main():

    """
        Delete the cluster environment in the AWS .

    """

    redshift, iam = create_connection_aws_cluster()
    delete_aws_iam_role(iam)
    delete_aws_cluster(redshift)


if __name__ == "__main__":
    main()