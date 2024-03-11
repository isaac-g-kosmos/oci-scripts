# OCI Scriptps

# Set up
to se up your credential use the following code  after installing OCI:

oci setup oci-cli-rc --file path/to/target/file
# Fies
- [copy_files.py](copy_files.py): Script for copying from a dir in OCI buckets to anothhe OCI bucket
- [create_csv.py](create_csv.py): Create csv for use in [data_upload_script_csv.py](data_upload_script_csv.py)
- [data_transfer_script.py](data_transfer_script.py): Script for transferring from S3 to OCI
- [data_upload_script.py](data_upload_script.py): Script for uploading from you CLI
- [data_upload_script_csv.py](data_upload_script_csv.py): Script for uploading from a csv
  - [delete_dir.py](delete_dir.py): Script for deleting file from an OCI bucket
- [download_files_OCI.py](download_files_OCI.py): Download files from OCI to a local dir
- [restore_2_standard.py](restore_2_standard.py): Restore file in a "Archived"  storage tier to "Standard"
- [restore_scripts.py](restore_scripts.py): Restore script for individual file
