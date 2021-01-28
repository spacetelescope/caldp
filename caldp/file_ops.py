import sys
import os
import glob
import tarfile
import boto3
import threading
from caldp import process


def get_local_outpath(output_uri, ipppssoot):
    """Returns full path to folder containing output files.
    """
    if output_uri.startswith("s3"):
        local_outpath = process.get_output_path("file:outputs", ipppssoot)
    else:
        local_outpath = process.get_output_path(output_uri, ipppssoot)
    return os.path.abspath(local_outpath)


def find_files(file_path):
    search_fits = f"{file_path}/*.fits"
    search_tra = f"{file_path}/*.tra"
    search_prev = f"{file_path}/previews/*"
    file_list = list(glob.glob(search_fits))
    file_list.extend(list(glob.glob(search_tra)))
    file_list.extend(list(glob.glob(search_prev)))
    return file_list


def make_tar(file_list, file_path, ipppssoot):
    working_dir = os.getcwd()
    os.chdir(file_path)
    tar_name = ipppssoot + ".tar.gz"
    print("Creating tarfile: ", tar_name)
    if os.path.exists(tar_name):
        os.remove(tar_name)  # clean up from prev attempts
    with tarfile.open(tar_name, "x:gz") as tar:
        for f in file_list:
            tar.add(f)
    tar_dest = os.path.abspath(tar_name)
    print("Tar successful: ", tar_dest)
    os.chdir(working_dir)
    return tar_dest


def upload_tar(tar, output_path):
    client = boto3.client("s3")
    parts = output_path[5:].split("/")
    bucket, prefix = parts[0], "/".join(parts[1:])
    objectname = prefix + "/" + os.path.basename(tar)
    print(f"Uploading: s3://{bucket}/{objectname}")
    with open(tar, "rb") as f:
        client.upload_fileobj(f, bucket, objectname, Callback=ProgressPercentage(tar))


class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write("\r%s  %s / %s  (%.2f%%)" % (self._filename, self._seen_so_far, self._size, percentage))
            sys.stdout.flush()


# def clean_up(file_list, ipppssoot, dirs=None):
#     print("Cleaning up...")
#     for f in file_list:
#         try:
#             os.remove(f)
#         except FileNotFoundError:
#             pass
#     if dirs is not None:
#         for d in dirs:
#             subdir = os.path.abspath(f"outputs/{ipppssoot}/{d}")
#             os.rmdir(subdir)
#     print("Done.")


def tar_outputs(ipppssoot, output_uri):
    output_path = process.get_output_path(output_uri, ipppssoot)
    local_outpath = get_local_outpath(output_uri, ipppssoot)
    file_list = find_files(local_outpath)
    tar = make_tar(file_list, local_outpath, ipppssoot)
    upload_tar(tar, output_path)
    #clean_up(file_list, ipppssoot, dirs=["previews"])
