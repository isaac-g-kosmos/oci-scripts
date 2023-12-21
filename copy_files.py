import oci


def move_files(source_namespace, source_bucket, source_directory, destination_namespace, destination_bucket, destination_directory, config,destructive=False):
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
        if list_objects_response.data.next_start_with == None:
            break
        page += 1
    # Iterate through each object in the source directory
    object_list.reverse()
    for obj in object_list:
        source_object_name = obj.name
        destination_object_name = source_object_name.replace(source_directory, destination_directory, 1)

        # Construct the source and destination object URLs
        source_object_url = f"https://objectstorage.{source_namespace}.oraclecloud.com/n/{source_namespace}/b/{source_bucket}/o/{source_object_name}"
        destination_object_url = f"https://objectstorage.{destination_namespace}.oraclecloud.com/n/{destination_namespace}/b/{destination_bucket}/o/{destination_object_name}"

        # Copy the object to the new location
        copy_object_response = object_storage_client.copy_object(
            namespace_name=destination_namespace,
            bucket_name=destination_bucket,
            # source_object_url,
            copy_object_details=oci.object_storage.models.CopyObjectDetails(
                source_object_name=source_object_name,
                # source_object_if_match_e_tag=obj.etag,
                # source_version_id=,
                # destination_object_if_match_e_tag=,
                # destination_object_if_none_match_e_tag=,
                # destination_object_metadata={
                #     'EXAMPLE_KEY_Itpug': 'EXAMPLE_VALUE_Ruz1ZzIwDZCsgLkgel2G'},

                destination_region="mx-queretaro-1",
                destination_namespace=destination_namespace,
                destination_bucket=destination_bucket,
                destination_object_name=destination_object_name,
                destination_object_storage_tier="Archive"
            )
        )

        # Check if the copy was successful
        if copy_object_response.status == 202 :
            print(f"Object copied successfully from {source_object_url} to {destination_object_url}")
            if destructive:
                # Delete the original object
                delete_object_response = object_storage_client.delete_object(
                    source_namespace,
                    source_bucket,
                    source_object_name
                )

                # Check if the deletion was successful
                if delete_object_response.status == 204:
                    print(f"Original object deleted successfully: {source_object_url}")
                else:
                    print(f"Failed to delete the original object: {source_object_url}")
        else:
            print(f"Failed to copy object from {source_object_url} to {destination_object_url}")




if __name__ == "__main__":

    config = oci.config.from_file()

    path_names = [
                  "NUAA",

                  "Youtube",
                  "Youtube_vlogs",
                  "classified_vlogs",
                  "HPAD",
                  "kosmos",
                  "models", "Celeb"]

    source_namespace = "axnq1wbomszp"
    source_bucket = "Spoof-datasets"
    destination_namespace = "axnq1wbomszp"
    destination_bucket = "Spoof-datasets"

    for source_object_name in path_names:
        # source_object_name = "HPAD"

        destiation_name=source_object_name.split("/")[-1].lower().replace("-","_")
        destination_object_name = f"spoof_detection/{destiation_name}"


        move_files(source_namespace, source_bucket, source_object_name, destination_namespace, destination_bucket,
                    destination_object_name, config,destructive=False)
