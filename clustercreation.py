import argparse
import os
import re

from google.cloud import dataproc_v1
from gcloud import storage
sa_acct_json_dir_path = os.getcwd()
sa_acct_json_path=sa_acct_json_dir_path+"/key.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS']=sa_acct_json_path

'''
project_id='mar2024project'
region="us-west1"
cluster_name="trainingclust"
# Create the cluster client.
#region, project_id,cluster_name
'''

def createcluster(clusterinputs):
    print("Starting job execution")
    region=clusterinputs['region']
    endpoint=region+"-dataproc.googleapis.com:443"
    cluster_client = dataproc_v1.ClusterControllerClient(
        client_options={"api_endpoint": endpoint}
    )
    # Create the cluster config.
    print("In cluster config")
    cluster = {
        "project_id": clusterinputs['project_id'],
        "cluster_name": clusterinputs['cluster_name'],
        "config": {
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        },
    }

    print("creating cluster....")
    # Create the cluster.
    operation = cluster_client.create_cluster(
        request={"project_id": clusterinputs['project_id'], "region": clusterinputs['region'], "cluster": cluster}
    )
    result = operation.result()


    print(f"Cluster created successfully: {result.cluster_name}")