import oci
import boto3

# AWS S3 Configuration
s3_bucket_name = 'dataset-upload-kosmos'

# OCI Configuration
oci_config = oci.config.from_file()  # Assumes OCI configuration is set up

# Initialize AWS S3 client
s3_client = boto3.client('s3')

# Initialize OCI Object Storage client
object_storage = oci.object_storage.ObjectStorageClient(oci_config)

# Define the source S3 bucket and destination OCI bucket
source_s3_bucket = s3_bucket_name
destination_oci_bucket = 'Spoof-datasets'

# Initialize variables for pagination
next_continuation_token = None
def does_object_exist(bucket_name, object_name):
    try:
        object_storage.get_object(
            namespace_name="axnq1wbomszp",
            bucket_name=bucket_name,
            object_name=object_name
        )
        return True
    except oci.exceptions.ServiceError as e:
        if e.status == 404:
            return False
        else:
            raise
## %%

while True:
    # List objects in the source S3 bucket with pagination
    list_objects_params = {
        'Bucket': source_s3_bucket,
        'MaxKeys': 1000,  # Maximum number of objects per response
        'Prefix': "WFLW",
      # 'Reverse': True
    }

    if next_continuation_token:
        print("-------------",next_continuation_token)
        list_objects_params['ContinuationToken'] = next_continuation_token

    s3_objects_response = s3_client.list_objects_v2(**list_objects_params)

    # Iterate through S3 objects and copy them to OCI Object Storage
    for s3_object in s3_objects_response.get('Contents', []):
        s3_object_key = s3_object['Key']

        # Check if the file exists in OCI Object Storage
        if not does_object_exist(destination_oci_bucket, s3_object_key):
            # Download the object from S3
            response = s3_client.get_object(Bucket=source_s3_bucket, Key=s3_object_key)
            s3_object_data = response['Body'].read()

            # Upload the object to OCI Object Storage
            object_storage.put_object(
                namespace_name="axnq1wbomszp",
                bucket_name=destination_oci_bucket,
                object_name=s3_object_key,
                put_object_body=s3_object_data
            )

            print(f"Transferred {s3_object_key} to OCI Object Storage")
        else:
            print(f"{s3_object_key} already exists in OCI Object Storage")

    # Check if there are more objects to paginate through
    next_continuation_token = s3_objects_response.get('NextContinuationToken')
    if not next_continuation_token:
        break

print("File transfer completed.")


    #%%
