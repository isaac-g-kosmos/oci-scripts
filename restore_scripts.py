import oci
import argparse
from typing import Optional

def restore_object(object_name: str, bucket_name: str, namespace_name: str, object_storage_client: oci.object_storage.ObjectStorageClient) -> None:
    """
    Initiate restoration process for a specified object in OCI Object Storage.

    :param object_name: The name of the object to be restored.
    :param bucket_name: The name of the bucket containing the object to be restored.
    :param namespace_name: The namespace (tenancy) ID.
    :param object_storage_client: An initialized OCI ObjectStorageClient instance.
    :return: None
    """
    try:
        # Initiate the restoration process
        response = object_storage_client.restore_objects(
            namespace_name=namespace_name,
            bucket_name=bucket_name,
            restore_objects_details=oci.object_storage.models.RestoreObjectsDetails(
                object_name=object_name,
                hours=24, ),
        )

        print(f"Restoration request successful. Work request ID: {response}")
    except oci.exceptions.ServiceError as e:
        print(f"Error initiating restoration: {e}")

def main() -> None:
    """
    Parse command line arguments and initiate restoration process for the specified object.

    :return: None
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--oci-bucket', help='Bucket for files to be uploaded', type=str)
    parser.add_argument('--compartment-id', help='OCI id for bucket compartment', type=str)
    parser.add_argument('--object-name', help='OCI path to object to restore', type=str)
    parser.add_argument('--name-space', help='OCI namespace being used', type=str)

    args = parser.parse_args()
    object_name: Optional[str] = args.object_name
    bucket_name: Optional[str] = args.oci_bucket
    namespace: Optional[str] = args.name_space
    config = oci.config.from_file()
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    restore_object(object_name, bucket_name, namespace, object_storage_client)

if __name__ == "__main__":
    main()
