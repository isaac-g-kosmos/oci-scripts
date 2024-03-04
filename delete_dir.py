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
        break
        if list_objects_response.data.next_start_with == None:
            break
        page += 1
    object_list= [obj.name for obj in object_list]
    object_list= [obj.removeprefix(prefix) for obj in object_list]
    return object_list

bucket_name = "Spoof-datasets"
namespace ="axnq1wbomszp"
source_object_name="VGG_FACE"
delete_dir=list_oci_directory(namespace,bucket_name,source_object_name)
delete_dir.reverse()
for obj_name in delete_dir:
    try:

        obj_name=source_object_name+obj_name
    #%%
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
    except:
        pass