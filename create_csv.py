import os
import csv
import argparse
from typing import List


def list_all_paths(directory: str) -> List[str]:
    """
    List all file paths in the given directory and its subdirectories.

    :param directory: The directory to search for files.
    :return: A list of file paths found in the directory and its subdirectories.
    """
    file_paths = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths


def main() -> None:
    """
    Parse command line arguments and split file paths into multiple CSV files.

    :return: None
    """
    parser = argparse.ArgumentParser(
        description='List all files in a directory in multiple CSV files, meant to be used in conjunction with '
                    'data_upload_script_csv.py')
    parser.add_argument('--directory', help='Path to the directory containing the files', type=str)
    parser.add_argument('--output-directory', help='Path to the directory where CSV files will be saved', type=str)
    parser.add_argument('--num-files', help='Number of files to split the directory into', type=int)
    args = parser.parse_args()
    directory: str = args.directory
    output_directory: str = args.output_directory
    num_files: int = args.num_files

    # List all paths in the directory
    paths = list_all_paths(directory)

    # Calculate the number of paths per file
    paths_per_file = len(paths) // num_files

    # Split the list of paths into separate lists
    split_paths = [paths[i:i + paths_per_file] for i in range(0, len(paths), paths_per_file)]

    # Save the paths to separate CSV files
    for i, path_list in enumerate(split_paths):
        csv_file = os.path.join(output_directory, f'file_paths_{i + 1}.csv')
        with open(csv_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["File Paths"])
            for path in path_list:
                writer.writerow([path])

        print(f"File paths saved to {csv_file}")


if __name__ == "__main__":
    main()
