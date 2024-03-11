import oci
import argparse

from retry import retry

def list_files(config,source_namespace,source_bucket,source_directory):
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
        if list_objects_response.data.next_start_with == None:
            break
        page += 1

    return object_list

def restore_object(config,object_name, bucket_name, namespace_name):
    # Load the OCI configuration from the default location

    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    # Get the namespace (tenancy) ID

    try:
        # Initiate the restoration process
        response = object_storage_client.restore_objects(
            namespace_name=namespace_name,
            bucket_name=bucket_name,
            restore_objects_details=oci.object_storage.models.RestoreObjectsDetails(
                object_name=object_name,
                hours=24,),
            )

        print(f"Restoration request successful. Work request ID: {response.data}")
    except oci.exceptions.ServiceError as e:
        print(f"Error initiating restoration: {e}")


@retry(tries=30,  delay=120)
def copy_to_same_path(config,object_name, bucket_name, namespace_name):
    object_storage_client = oci.object_storage.ObjectStorageClient(config)
    copy_object_response = object_storage_client.copy_object(
        namespace_name=namespace_name,
        bucket_name=bucket_name,
        # source_object_url,
        copy_object_details=oci.object_storage.models.CopyObjectDetails(
            source_object_name=object_name,
            # source_object_if_match_e_tag=obj.etag,
            # source_version_id=,
            # destination_object_if_match_e_tag=,
            # destination_object_if_none_match_e_tag=,
            # destination_object_metadata={
            #     'EXAMPLE_KEY_Itpug': 'EXAMPLE_VALUE_Ruz1ZzIwDZCsgLkgel2G'},

            destination_region="mx-queretaro-1",
            destination_namespace=namespace_name,
            destination_bucket=bucket_name,
            destination_object_name=object_name,
            destination_object_storage_tier="Standard"
        )
    )

    # Check if the copy was successful
    if copy_object_response.status == 202:
        print(f"Object copied successfully from {object_name} to {object_name}")

    else:
        print(f"Failed to copy object from {object_name} to {object_name}")
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--oci-bucket', help='Bucket for files to be uploaded')
    parser.add_argument('--restore-path', help='OCI path to directory of objets to be restored')
    parser.add_argument('--name-space', help='OCI namespace being used')
    args = parser.parse_args()
    config = oci.config.from_file()
    source_namespace = args.name_space
    source_bucket = args.oci_bucket
    restore_path=args.restore_path

    files_to_convert=list_files(config,source_namespace,source_bucket,restore_path)

    for obj in files_to_convert:
        try:
            restore_object(config,obj.name,source_bucket,source_namespace)
        except:
            print(f"Object cant be restored {obj.name}")

    for obj in files_to_convert:
            copy_to_same_path(config,obj.name,source_bucket,source_namespace)
