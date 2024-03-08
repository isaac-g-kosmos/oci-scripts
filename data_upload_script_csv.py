import os
import oci
import pandas as pd
import argparse
from typing import List, Dict

config = oci.config.from_file()
object_storage = oci.object_storage.ObjectStorageClient(config)

def upload_to_oci(oci_bucket_name: str, local_file_path: str, namespace: str, prefix: str) -> None:
    """
    Upload a file to OCI Object Storage.

    :param oci_bucket_name: Name of the OCI bucket.
    :param local_file_path: Local path of the file to be uploaded.
    :param namespace: OCI namespace being used.
    :param prefix: Path where the files would be uploaded in OCI.
    :return: None
    """
    oci_object_name = prefix + os.path.basename(local_file_path)
    print("oci object", oci_object_name)
    object_storage = oci.object_storage.ObjectStorageClient(config)

    create_multipart_upload_response = object_storage.create_multipart_upload(
        namespace_name=namespace,
        bucket_name=oci_bucket_name,
        create_multipart_upload_details=oci.object_storage.models.CreateMultipartUploadDetails(
            object=oci_object_name
        )
    )

    upload_id = create_multipart_upload_response.data.upload_id

    part_size_bytes = 1 * 1024 * 1024  # 1MB

    upload_parts = []

    with open(local_file_path, 'rb') as file:
        part_number = 1
        while True:
            part_data = file.read(part_size_bytes)
            if not part_data:
                break

            upload_part_response = object_storage.upload_part(
                namespace_name=namespace,
                bucket_name=oci_bucket_name,
                object_name=oci_object_name,
                upload_id=upload_id,
                upload_part_num=part_number,
                upload_part_body=part_data
            )

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


def delete_file(local_path: str) -> None:
    """
    Delete a file from the local directory.

    :param local_path: Path of the file to be deleted.
    :return: None
    """
    os.remove(local_path)


def main() -> None:
    """
    Main function to upload files to OCI Object Storage and delete them from the local directory.

    :return: None
    """
    parser = argparse.ArgumentParser(description='Upload files to OCI Object Storage and delete them from the local directory.')
    parser.add_argument('--csv_path', help='Path to the CSV file containing the list of file paths')
    parser.add_argument('--local-dir', help='Root dir for files')
    parser.add_argument('--name-space', help='OCI namespace being used')
    parser.add_argument('--bucket', help='Bucket for files to be uploaded')
    parser.add_argument('--prefix', help='Path where the files would be uploaded in OCI')
    args = parser.parse_args()

    local_directory: str = args.local_dir
    namespace: str = args.name_space
    bucket_name: str = args.bucket
    csv_file_path: str = args.csv_path
    prefix: str = args.prefix

    # Read the list of file paths from the CSV file
    file_paths_df = pd.read_csv(csv_file_path)
    file_paths: List[str] = file_paths_df["File Paths"].tolist()

    # Walk through the local directory and upload files
    for file_path in file_paths:
        local_path = file_path
        print(local_path)
        object_name = os.path.relpath(local_path, local_directory)
        upload_to_oci(bucket_name, local_path, namespace, prefix)
        delete_file(local_path)
        print(f"Uploaded and deleted: {object_name}")

    print("Upload and delete complete.")


if __name__ == "__main__":
    main()
