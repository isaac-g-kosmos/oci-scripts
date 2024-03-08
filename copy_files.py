import argparse
from typing import Dict

import oci

def move_files(source_namespace: str, source_bucket: str, source_directory: str, destination_namespace: str, destination_bucket: str, destination_directory: str, config: Dict, destructive: bool = False) -> None:
    """
    Move files from one OCI bucket to another location in OCI buckets.

    :param source_namespace: Name space of origin for files.
    :param source_bucket: Bucket of origin for files.
    :param source_directory: Directory of origin for files.
    :param destination_namespace: Name space of target for files.
    :param destination_bucket: Bucket of target for files.
    :param destination_directory: Directory of target for files.
    :param config: OCI configuration details.
    :param destructive: If True, deletes all files that are uploaded (default is False).
    :return: None
    """
    # Create a new Object Storage client
    object_storage_client = oci.object_storage.ObjectStorageClient(config)

    # List all objects in the source directory
    object_list = []
    list_objects_response = object_storage_client.list_objects(
        source_namespace, source_bucket, prefix=source_directory
    )
    object_list.extend(list_objects_response.data.objects)
    page = 1
    while True:
        print(page)

        list_objects_response = object_storage_client.list_objects(
            source_namespace, source_bucket, prefix=source_directory, start=list_objects_response.data.next_start_with
        )
        object_list.extend(list_objects_response.data.objects)
        if list_objects_response.data.next_start_with is None:
            break
        page += 1
    # Iterate through each object in the source directory
    object_list.reverse()
    for obj in object_list:
        source_object_name = obj.name
        destination_object_name = source_object_name.replace(source_directory, destination_directory, 1)

        # Copy the object to the new location
        copy_object_response = object_storage_client.copy_object(
            namespace_name=destination_namespace,
            bucket_name=destination_bucket,
            copy_object_details=oci.object_storage.models.CopyObjectDetails(
                source_object_name=source_object_name,
                destination_region="mx-queretaro-1",
                destination_namespace=destination_namespace,
                destination_bucket=destination_bucket,
                destination_object_name=destination_object_name,
                destination_object_storage_tier="Archive"
            )
        )

        # Check if the copy was successful
        if copy_object_response.status == 202:
            print(f"Object copied successfully from {source_object_name} to {destination_object_name}")
            if destructive:
                # Delete the original object
                delete_object_response = object_storage_client.delete_object(
                    source_namespace,
                    source_bucket,
                    source_object_name
                )

                # Check if the deletion was successful
                if delete_object_response.status == 204:
                    print(f"Original object deleted successfully: {source_object_name}")
                else:
                    print(f"Failed to delete the original object: {destination_object_name}")
        else:
            print(f"Failed to copy object from {source_object_name} to {destination_object_name}")

def main() -> None:
    """
    Parse command line arguments and initiate the file moving process.

    :return: None
    """
    parser = argparse.ArgumentParser(description='Move files directories from one OCI bucket another location in OCI buckets')
    parser.add_argument('--source-namespace', help='Name space of origin for files', type=str)
    parser.add_argument('--destination-namespace', help='Name space of target for files', type=str)
    parser.add_argument('--source-bucket', help='Bucket of origin for files', type=str)
    parser.add_argument('--destination-bucket', help='Bucket of target for files', type=str)
    parser.add_argument('--source-dir', help='Directory of origin for files', type=str)
    parser.add_argument('--destination-dir', help='Directory of target for files', type=str)
    parser.add_argument('--destructive', help='If True deletes all files that are uploaded', default=False, type=bool)
    args = parser.parse_args()

    source_namespace = args.source_namespace
    source_bucket = args.source_bucket
    destination_namespace = args.destination_namespace
    destination_bucket = args.destination_bucket

    source_object_dir = args.source_dir
    destination_object_dir = args.destination_dir

    move_files(source_namespace, source_bucket, source_object_dir, destination_namespace, destination_bucket,
                destination_object_dir, config, destructive=False)

if __name__ == "__main__":
    main()
