import gcloud
from gcloud import storage
from google.cloud import bigquery
import os, json
import clustercreation, submitjob 

def createBucket(data):
    client = storage.Client(project=data['project_id'])
    #create bucket
    #bucket = client.create_bucket(data['bucket_name'])
    print("Bucket "+data['bucket_name']+" successfully created in project : "+data['project_id'])

## Copy files from hdfs to gcs
def copyfilestoGCS(hdfs_distcp_cmd_var, bucket_name):
    hdfs_cp_cmd='hdfs dfs -cp '
    gs_cmd='gs:// '
    hdfs_cp_cmd+hdfs_distcp_cmd_var+gs_cmd+bucket_name

    
##
def filetoBQ_datacopy(data, sa_acct_json_path):
    print  ("Authenticating gcp before transferring data to bq......")

    BQ_CLIENT = bigquery.Client().from_service_account_json(sa_acct_json_path)
    TABLE_ID=data['project_id']+'.'+data['bqdataset_name']+"."+data['bqtable_name']
    #print(BQ_CLIENT)
    print("End....")
    print("Start Schema Setting....")

    hdw_schema = BQ_CLIENT.get_table(TABLE_ID)
    print("Hdw Schema: ")
    print(hdw_schema.schema)
    for field in hdw_schema.schema:
        print(type(field))
        SCHEMA_LIST.append(bigquery.SchemaField(name=field.name, field_type=field.field_type, mode=field.mode))

    print(SCHEMA_LIST[0])

    print("BQ Load starts.....")

    job_config = bigquery.LoadJobConfig(skip_leading_rows=1)
    job_config.schema = SCHEMA_LIST
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = ','
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.null_marker = ''

    gcs_uri = 'gs://mar2024bkt/movies/movies.csv'


    load_job = BQ_CLIENT.load_table_from_uri(
        gcs_uri, TABLE_ID, job_config=job_config
    )

    load_job.result() # waits for the load job to finish

    destination_table = BQ_CLIENT.get_table(TABLE_ID)

    print(f'Loaded {destination_table.num_rows} rows in table')

if __name__ == "__main__":
    ##
    #Global variables
    global BQ_CLIENT

    #Variables
    SCHEMA_LIST = []
    clusterinputs = {}
    jobinputs = {}
    sa_acct_json_dir_path = os.getcwd()
    sa_acct_json_path=sa_acct_json_dir_path+"/key.json"

    json_path=os.getcwd()
    json_file_name='sample.json'
    json_file=json_path+json_file_name

    jsondata=open(json_file)
    data=json.load(jsondata)
    json_file_gcs='gs://'+data['bucket_name']+'/'

    hdfs_main_location=data['src_hdfs_table_location']
    hdfs_db_name=data['hdfs_db_name']+".db/"
    hdfs_table_name=data['hdfs_table_name']
    hdfs_distcp_cmd_var=hdfs_main_location

    #Authentication
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']=sa_acct_json_path

    ## Create Bucket
    createBucket(data)
    copyfilestoGCS(hdfs_distcp_cmd_var,data['bucket_name'])

    if data['load_from'] == 'file':
        filetoBQ_datacopy(data, sa_acct_json_path)
    else:
        clusterinputs['region'] = 'us-west1'
        clusterinputs['project_id'] = data['project_id']
        clusterinputs['cluster_name'] = data['cluster_name']

        #clustercreation.createcluster(clusterinputs)

        jobinputs['sa_credens'] = sa_acct_json_path
        jobinputs['region'] = 'us-west1'
        jobinputs['project_id'] = data['project_id']
        jobinputs['cluster_name'] = data['cluster_name']
        jobinputs['python_file'] = "gs://"+data['bucket_name']+"/"+data['tobe_executed_py_filename'],
        jobinputs['json_file_path_local'] = json_file
        jobinputs['json_file_path_gcs'] = json_file_gcs
        jobinputs['bucket_name'] = data['bucket_name']
        jobinputs['bqdataset_name'] = data['bqdataset_name']
        jobinputs['bqtable_name'] = data['bqtable_name']
        jobinputs['hdfs_table_name'] = data['hdfs_table_name']

        
        client = storage.Client(project=data['project_id'])
        bucket = client.get_bucket(data['bucket_name'])
        blob = bucket.blob(json_file_name)
        blob.upload_from_filename(json_file)

        submitjob.jobsubmit(jobinputs)