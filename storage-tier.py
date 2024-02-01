import oci
import oci

# Create a default config using DEFAULT profile in default location
# Refer to
# https://docs.cloud.oracle.com/en-us/iaas/Content/API/Concepts/sdkconfig.htm#SDK_and_CLI_Configuration_File
# for more info
config = oci.config.from_file()


# Initialize service client with default config file
object_storage_client = oci.object_storage.ObjectStorageClient(config)

bucket_name = "Spoof-datasets"
namespace = "axnq1wbomszp"
compartment_id = "ocid1.compartment.oc1..aaaaaaaaelv42hlt455pwfprsf7tjwso5vlbsev4kc6pvhhxtljedpfccr5a"
object_name = "spoof_detection/celeb/1/live/032887.jpg"
# object_name = "Celeb/1/live/000184.jpg"

# Send the request to service, some parameters are not required, see API
# doc for more info
#%%
update_object_storage_tier_response = object_storage_client.update_object_storage_tier(
    namespace_name=namespace,
    bucket_name=bucket_name,
    update_object_storage_tier_details=oci.object_storage.models.UpdateObjectStorageTierDetails(
        object_name=object_name ,#+ '/'+object_name,
        # storage_tier='Standard',
        storage_tier='InfrequentAccess',
        # version_id="ocid1.test.oc1..<unique_ID>EXAMPLE-versionId-Value"
    ),
    # opc_client_request_id="1"
    )
# Get the data from response
print(update_object_storage_tier_response.headers)