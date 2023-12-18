import oci

def copy_object_between_buckets(config, source_namespace, source_bucket, source_object, dest_namespace, dest_bucket, dest_object):
    # Initialize the Object Storage client
    object_storage = oci.object_storage.ObjectStorageClient(config)

    # Get the source object details
    get_object_response = object_storage.get_object(
        source_namespace,
        source_bucket,
        source_object
    )

    # Copy the object to the destination bucket
    copy_object_response = object_storage.copy_object(
        source_namespace,
        source_bucket,
        source_object,
        dest_namespace,
        dest_bucket,
        dest_object,
        copy_object_details=oci.object_storage.models.CopyObjectDetails()
    )

    print(f"Object '{source_object}' copied from '{source_bucket}' to '{dest_bucket}' as '{dest_object}'")
    print("Copy operation details:", copy_object_response.data)

if __name__ == "__main__":
    # Replace these values with your OCI credentials and bucket details
    config = oci.config.from_file(r"C:\Users\isaac\PycharmProjects\OCI-scripts\config")
    source_namespace = "axnq1wbomszp"
    source_bucket = "Spoof-datasets"
    source_object = "/home/ubuntu/scrapped_picutures/0.png"
    dest_namespace = "axnq1wbomszp"
    dest_bucket = "Spoof-datasets"
    dest_object = "scrapped_picutures/0.png"

    copy_object_between_buckets(config, source_namespace, source_bucket, source_object, dest_namespace, dest_bucket, dest_object)
