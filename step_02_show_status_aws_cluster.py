import boto3
import configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY      = config.get('AWS','KEY')
SECRET  = config.get('AWS','SECRET')

DWH_CLUSTER_IDENTIFIER = config.get('DWH','DWH_CLUSTER_IDENTIFIER')

def main():

    """
        Show the Current Status of the Cluster
    
    """


    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        )

    print(redshift.describe_clusters())



if __name__ == "__main__":
    main()