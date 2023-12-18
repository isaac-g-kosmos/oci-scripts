import os
import csv

directory = '/home/ubuntu/scrapped_picutures'  # Replace with the path to your directory
output_directory = '/home/ubuntu'  # Replace with the directory where you want to save CSV files
num_files = 2  # Number of CSV files to create

# Function to list all paths in a directory
def list_all_paths(directory):
    file_paths = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths

# List all paths in the directory
paths = list_all_paths(directory)

# Calculate the number of paths per file
paths_per_file = len(paths) // num_files

# Split the list of paths into four lists
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