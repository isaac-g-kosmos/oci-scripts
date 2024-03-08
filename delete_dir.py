import oci
from typing import List
import argparse


def list_oci_directory(namespace: str, bucket_name: str, prefix: str) -> List[str]:
    """
    List objects in an OCI Object Storage bucket with a specific prefix.

    :param namespace: OCI namespace being used.
    :param bucket_name: Name of the OCI bucket.
    :param prefix: Prefix for the objects.
    :return: List of object names.
    """
    object_list = []
    list_objects_response = object_storage.list_objects(
        namespace, bucket_name, prefix=prefix
    )
    object_list.extend(list_objects_response.data.objects)
    page = 1
    while True:
        print(page)

        list_objects_response = object_storage.list_objects(
            namespace, bucket_name, prefix=prefix, start=list_objects_response.data.next_start_with
        )

        object_list.extend(list_objects_response.data.objects)
        if list_objects_response.data.next_start_with is None:
            break
        page += 1

    object_list = [obj.name for obj in object_list]
    object_list = [obj.removeprefix(prefix) for obj in object_list]
    return object_list


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--oci-bucket', help='Bucket for files to be uploaded')
    parser.add_argument('--prefix', help='Path where the files would be uploaded in OCI')
    parser.add_argument('--name-space', help='OCI namespace being used')
    args = parser.parse_args()

    bucket_name = args.oci_bucket
    namespace = args.name_space
    prefix = args.prefix

    config = oci.config.from_file()
    object_storage = oci.object_storage.ObjectStorageClient(config)

    delete_dir = list_oci_directory(namespace, bucket_name, prefix)
    delete_dir.reverse()

    for obj_name in delete_dir:
        try:
            obj_name = prefix + obj_name
            delete_object_response = object_storage.delete_object(
                namespace,
                bucket_name,
                obj_name
            )
            # Check if the deletion was successful
            if delete_object_response.status == 204:
                print(f"Original object deleted successfully: {obj_name}")
            else:
                print(f"Failed to delete the original object: {obj_name}")
        except Exception as e:
            print(f"Failed to delete the object: {obj_name}. Error: {str(e)}")


if __name__ == "__main__":
    main()
