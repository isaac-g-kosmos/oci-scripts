import oci
import os
# Replace these with your own values
config = {
    "user": "ocid1.user.oc1..aaaaaaaaguhfrbhcdmpg422gmy26a26wgjrlfmksn4s6hwkxkjbsso27bida",
    "key_file": "/home/ubuntu/spoof-detection/key.pem",
    "fingerprint": "95:bc:20:31:0a:62:f2:71:55:3d:13:e8:2f:5a:90:18",
    "tenancy": "ocid1.tenancy.oc1..aaaaaaaapl5yemd6dbgblyanwegoexml2ognad3djdz76rcozmsmeacmyntq",
    "region": "mx-queretaro-1",
}
object_storage = oci.object_storage.ObjectStorageClient(config)

# Specify the bucket name and directory you want to download from
bucket_name = "Spoof-datasets"
directory_name = "NUAA/images/mapping_cut/"  # Optional: leave empty for the root of the bucket

# Specify the local directory where you want to save the downloaded objects
local_directory = "/home/ubuntu/NUAA/mapping_cut"

# List objects in the specified directory

namespace ="axnq1wbomszp"
object_list = []
list_objects_response = object_storage.list_objects(
    namespace, bucket_name, prefix=directory_name
)
object_list.extend(list_objects_response.data.objects)
page = 1
while True:
    print(page)

    list_objects_response = object_storage.list_objects(
        namespace, bucket_name, prefix=directory_name, start=list_objects_response.data.next_start_with
    )
    object_list.extend(list_objects_response.data.objects)
    if list_objects_response.data.next_start_with == None:
        break
    page += 1

# Download each object in the directory
for obj in object_list:
    object_name1 = obj.name
    # print(object_name1)
    object_name=object_name1.replace("/home/ubuntu/NUAA/mapping_cut/","")
    download_path = f"{local_directory}/{object_name}"

    dir=os.path.dirname(download_path)

    if os.path.exists(dir):
        pass
    else:
        os.makedirs(dir)
    try:
        if os.path.exists(download_path):
            pass
        else:
            with open(download_path, "wb") as f:
                response = object_storage.get_object(
                    namespace_name=namespace,
                    bucket_name=bucket_name,
                    object_name=object_name1,
                    range="bytes=0-",
                    # stream=f,
                )
                for chunk in response.data.raw:
                    f.write(chunk)
            print(f"Object '{object_name1}' downloaded to '{download_path}'")
    except:
        pass