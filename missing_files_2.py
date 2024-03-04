import oci
from typing import List
config = oci.config.from_file(r"C:\Users\isaac\PycharmProjects\OCI-scripts\config")
object_storage = oci.object_storage.ObjectStorageClient(config)


def list_oci_directory(namespace:str,bucket_name:str,prefix:str)->List:
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
        if list_objects_response.data.next_start_with == None:
            break
        page += 1
    object_list= [obj.name for obj in object_list]
    object_list= [obj.removeprefix(prefix) for obj in object_list]
    return object_list

bucket_name = "Spoof-datasets"
namespace ="axnq1wbomszp"
source_object_name="VGG_FACE"
original_dir=list_oci_directory(namespace,bucket_name,source_object_name)
destiation_name = source_object_name.split("/")[-1].lower().replace("-", "_")

destination_object_name = f"spoof_detection_preprocessing/{destiation_name}"

copy_dir=list_oci_directory(namespace,bucket_name,destination_object_name)
#%%
missing=(len(set(original_dir))-len(set(copy_dir)))
#%%
list1=list(missing)
list1.reverse()
for obj in list1:
    source_object_name = f"VGG_FACE"+obj
    destination_object_name = "spoof_detection/vgg_face"+obj

    copy_object_response = object_storage.copy_object(
        namespace_name=namespace,
        bucket_name=bucket_name,
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
            destination_namespace=namespace,
            destination_bucket=bucket_name,
            destination_object_name=destination_object_name,
            destination_object_storage_tier="Archive"
        )
    )

    # Check if the copy was successful
    if copy_object_response.status == 202:
        print(f"Object copied successfully from {source_object_name} to {destination_object_name}")

    else:
        print(f"Failed to copy object from {source_object_name} to {destination_object_name}")