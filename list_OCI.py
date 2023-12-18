import os
import shutil
def list_all_paths(directory):
    file_paths = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths

file_paths=list_all_paths("/home/ubuntu/NUAA/mapping_cut")

for file in file_paths:
    print(file)
    basename=os.path.basename(file)
    target=os.path.join("/home/ubuntu/NUAA/depth-map",basename)
    shutil.copy(file,target)