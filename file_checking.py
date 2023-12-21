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
#%%
bucket_name = "Spoof-datasets"
namespace ="axnq1wbomszp"
source_object_name="/home/ubuntu/scrapped_picutures"
original_dir=list_oci_directory(namespace,bucket_name,source_object_name)
#%%
destiation_name = source_object_name.split("/")[-1].lower().replace("-", "_")
destination_object_name = f"spoof_detection_preprocessing/{destiation_name}"

copy_dir=list_oci_directory(namespace,bucket_name,destination_object_name)

print(set(original_dir)==set(copy_dir))
