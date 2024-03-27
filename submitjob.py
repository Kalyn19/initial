from google.cloud import dataproc_v1 as dataproc
import os,json



def jobsubmit(jobinputs):
    print(jobinputs)
    sa_acct_json_dir_path = os.getcwd()
    sa_acct_json_path=sa_acct_json_dir_path+"/key.json"
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=sa_acct_json_path
    # Define your cluster details

    project_id = jobinputs['project_id']#'mar2024project'
    region = jobinputs['region']#'us-west1'
    cluster_name = jobinputs['cluster_name']#'trainingclust'
    job_file= jobinputs['python_file'][0]
    print('Job : '+job_file)
    # Initialize a Dataproc instance
    # sa_acct, region, project_id, clustername, python file gs path
    client = dataproc.JobControllerClient(client_options={
        'api_endpoint': 'us-west1-dataproc.googleapis.com:443'
    })
    print(client)

    
    # Prepare your pyspark job details
    job_payload = {
        'placement': {
            'cluster_name': cluster_name
        },
        'pyspark_job': {
            'main_python_file_uri': job_file,
            'args' : jobinputs,
            'jar_file_uris':['gs://mar2024bkt/spark-3.1-bigquery-0.36.1.jar']
        }
    }

    # Submit the job
    job_response = client.submit_job(project_id=project_id, region=region, job=job_payload)

    # Output a response
    print('Submitted job ID {}'.format(job_response.reference.job_id))

