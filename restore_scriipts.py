import oci

config = oci.config.from_file()

# Create Object Storage client
object_storage_client = oci.object_storage.ObjectStorageClient(config)
def restore_object(object_name, bucket_name, namespace_name, compartment_id):
    # Load the OCI configuration from the default location


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

        print(f"Restoration request successful. Work request ID: {response}")
    except oci.exceptions.ServiceError as e:
        print(f"Error initiating restoration: {e}")


# Specify the object details
#spoof_detection/classified_vlogs
bucket_name = "Spoof-datasets"
namespace = "axnq1wbomszp"
source_object_name = "VGG_FACE"
compartment_id = "ocid1.compartment.oc1..aaaaaaaaelv42hlt455pwfprsf7tjwso5vlbsev4kc6pvhhxtljedpfccr5a"
object_name = "spoof_detection/celeb/1/live/032887.jpg"

# Call the restore_object function
restore_object(object_name, bucket_name, namespace, compartment_id)
