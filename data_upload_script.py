import os
import oci
from retry import retry
# from oci.object_storage.models import CreateObjectDetails
import pandas as pd
import argparse

parser = argparse.ArgumentParser(description='Upload files to OCI Object Storage and delete them from the local directory.')
parser.add_argument('csv_path', help='Path to the CSV file containing the list of file paths')
args = parser.parse_args()

csv_file_path = args.csv_path

# Read the list of file paths from the CSV file
list = pd.read_csv(csv_file_path)
# list=pd.read_csv("/home/ubuntu/file_paths.csv")
list=list["File Paths"].tolist()
config = {
    "user": "ocid1.user.oc1..aaaaaaaaguhfrbhcdmpg422gmy26a26wgjrlfmksn4s6hwkxkjbsso27bida",
    "key_file": "/home/ubuntu/key.pem",
    "fingerprint": "95:bc:20:31:0a:62:f2:71:55:3d:13:e8:2f:5a:90:18",
    "tenancy": "ocid1.tenancy.oc1..aaaaaaaapl5yemd6dbgblyanwegoexml2ognad3djdz76rcozmsmeacmyntq",
    "region": "mx-queretaro-1",
    "compartment_id": "ocid1.compartment.oc1..aaaaaaaaelv42hlt455pwfprsf7tjwso5vlbsev4kc6pvhhxtljedpfccr5a",
}
namespace= "axnq1wbomszp"
# object_storage = oci.object_storage.ObjectStorageClient(config)

# Function to upload a file to Object Storage
@retry(tries=7, delay=1)
def upload_file(bucket_name, local_path, object_name):
    # create_details = CreateObjectDetails()
    # create_details.content_type = "application/octet-stream"  # You can set the appropriate content type.
    with open(local_path, "rb") as file:
        object_storage.put_object(namespace,bucket_name, object_name, file)
def upload_to_oci(oci_bucket_name,local_file_path, namespace):
    oci_object_name = local_file_path.replace("/home/ubuntu/NUAA/depth-map/","NUAA/images/mapping_cut/")
    print("oci object",oci_object_name )
    object_storage = oci.object_storage.ObjectStorageClient(config)

    # Determine the object name (key) in OCI based on the file name
    # oci_object_name = os.path.basename(oci_object_name)

    # Initialize multipart upload
    create_multipart_upload_response = object_storage.create_multipart_upload(
        namespace_name=namespace,
        bucket_name=oci_bucket_name,
        create_multipart_upload_details=oci.object_storage.models.CreateMultipartUploadDetails(
            object=oci_object_name
        )
    )

    upload_id = create_multipart_upload_response.data.upload_id

    # Define the part size (in bytes) for each part of the upload (1MB)
    part_size_bytes = 1 * 1024 * 1024  # 1MB

    # Initialize the upload parts list
    upload_parts = []

    # Open the file and split it into parts
    with open(local_file_path, 'rb') as file:
        part_number = 1
        while True:
            part_data = file.read(part_size_bytes)
            if not part_data:
                break

            # Upload each part
            upload_part_response = object_storage.upload_part(
                namespace_name=namespace,
                bucket_name=oci_bucket_name,
                object_name=oci_object_name,
                upload_id=upload_id,
                # part_number=part_number,  # Specify the part number
                upload_part_num=part_number,  # Specify the part number again
                upload_part_body=part_data
            )
            # print(upload_part_response)
            upload_parts.append(
                oci.object_storage.models.CommitMultipartUploadPartDetails(
                    part_num=part_number,
                    etag=upload_part_response.headers['etag']
                )
            )
            part_number += 1
    commit_multipart_upload_response = object_storage.commit_multipart_upload(
        namespace_name=namespace,
        bucket_name=oci_bucket_name,
        object_name=oci_object_name,
        upload_id=upload_id,
        commit_multipart_upload_details=oci.object_storage.models.CommitMultipartUploadDetails(
            parts_to_commit=upload_parts
        )
    )
    

# Function to delete a file after uploading
def delete_file(local_path):
    os.remove(local_path)

# Local directory to upload
local_directory = "/home/ubuntu/NUAA/depth-map"

# OCI Object Storage bucket to upload to
bucket_name = "Spoof-datasets"

# Walk through the local directory and upload files
for filename in list:
    local_path = filename
    print(local_path)
    object_name = os.path.relpath(local_path, local_directory)
    upload_to_oci(bucket_name, local_path, namespace)
    delete_file(local_path)
    print(f"Uploaded and deleted: {object_name}")

print("Upload and delete complete.")