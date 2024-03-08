import oci
import os
import argparse
# Replace these with your own values

def main():
    config = oci.config.from_file()

    object_storage = oci.object_storage.ObjectStorageClient(config)
    parser = argparse.ArgumentParser()
    parser.add_argument('--local_dir', 'Root dir for files to be uploaded')
    parser.add_argument('--oci-bucket', help='Bucket for files to be uploaded')
    parser.add_argument('--prefix', help='path where the files would be uploaded in OCI')
    parser.add_argument('--name-space', help='OCI namespace being used')
    args = parser.parse_args()
    bucket_name = args.oci_bucket
    directory_name = args.prefix

    local_directory = args.local_dir

    namespace = args.namespace





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

if __name__ == "__main__":
    main()

