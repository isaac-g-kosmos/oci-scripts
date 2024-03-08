import oci
import os
import argparse
from typing import Dict, List

import logging

app_logger = logging.getLogger("app")


def upload_to_oci(local_file_path: str, oci_object_name: str, oci_bucket_name: str, oci_config: Dict, namespace: str, prefix: str) -> None:
    """
    Upload a file to OCI Object Storage.

    :param local_file_path: Local path of the file to be uploaded.
    :param oci_object_name: Object name in OCI Object Storage.
    :param oci_bucket_name: Name of the OCI bucket.
    :param oci_config: OCI configuration details.
    :param namespace: OCI namespace being used.
    :param prefix: Path where the files would be uploaded in OCI.
    :return: None
    """
    oci_object_name = prefix + oci_object_name
    object_storage = oci.object_storage.ObjectStorageClient(oci_config)

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

    # Commit the upload
    commit_multipart_upload_response = object_storage.commit_multipart_upload(
        namespace_name=namespace,
        bucket_name=oci_bucket_name,
        object_name=oci_object_name,
        upload_id=upload_id,
        commit_multipart_upload_details=oci.object_storage.models.CommitMultipartUploadDetails(
            parts_to_commit=upload_parts
        )
    )


def list_oci_objects() -> List[str]:
    """
    List objects stored in OCI Object Storage.

    :return: List of object names.
    """
    if os.path.exists('files_list.txt'):
        with open('files_list.txt', 'r') as f:
            list_of_files = f.readlines()
        list_of_files = [x.replace('\n', '') for x in list_of_files]
        return list_of_files
    else:
        return []


def write_to_list(list_of_files: List[str]) -> None:
    """
    Write a list of object names to a text file.

    :param list_of_files: List of object names.
    :return: None
    """
    list_of_files = [x + '\n' for x in list_of_files]
    with open('files_list.txt', 'w') as f:
        f.writelines(list_of_files)


def sync_to_oci(local_dir: str, oci_bucket_name: str, oci_config: Dict, namespace: str, prefix: str) -> None:
    """
    Synchronize files from a local directory to OCI Object Storage.

    :param local_dir: Root directory for files to be uploaded.
    :param oci_bucket_name: Name of the OCI bucket.
    :param oci_config: OCI configuration details.
    :param namespace: OCI namespace being used.
    :param prefix: Path where the files would be uploaded in OCI.
    :return: None
    """
    oci_objects = list_oci_objects()

    for root, _, files in os.walk(local_dir):
        for file in files:
            local_file_path = os.path.join(root, file)
            oci_object_name = os.path.relpath(local_file_path, local_dir).replace(os.path.sep, '/')

            if oci_object_name not in oci_objects:
                print(f"Uploading {oci_object_name} to OCI Object Storage")
                upload_to_oci(local_file_path, oci_object_name, oci_bucket_name, oci_config, namespace, prefix)
                oci_objects.append(oci_object_name)
                write_to_list(oci_objects)
            else:
                print(f"{oci_object_name} already exists in OCI Object Storage")


def main() -> None:
    """
    Main function to synchronize files from a local directory to OCI Object Storage.

    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--local_dir', help='Root dir for files to be uploaded')
    parser.add_argument('--oci-bucket', help='Bucket for files to be uploaded')
    parser.add_argument('--prefix', help='Path where the files would be uploaded in OCI')
    parser.add_argument('--name-space', help='OCI namespace being used')

    args = parser.parse_args()

    prefix = args.prefix

    oci_config = oci.config.from_file()
    namespace = args.namespace

    sync_to_oci(args.local_dir, args.oci_bucket, oci_config, namespace, prefix)


if __name__ == "__main__":
    main()
