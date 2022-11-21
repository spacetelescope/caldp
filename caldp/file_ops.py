import sys
import os
import glob
import shutil
import tarfile
import boto3
import threading
from caldp import process
from caldp import log
from caldp import exit_codes
from caldp import sysexit


def s3_split_uri(uri):
    """
    >>> s3_split_uri('s3://the-bucket/prefix/parts/come/next')
    ('the-bucket', 'prefix/parts/come/next')

    >>> s3_split_uri('s3://the-bucket')
    ('the-bucket', '')
    """
    parts = uri[5:].split("/")
    bucket, prefix = parts[0], "/".join(parts[1:])
    # print("s3_split_uri:", uri, "->", bucket, prefix)
    return bucket, prefix


def get_input_path(input_uri, dataset, make=False):
    """Fetches the path to input files"""
    cwd = os.getcwd()
    if input_uri.startswith("file"):
        input_path = input_uri.split(":")[-1]
    else:
        input_path = os.path.join(cwd, "inputs", dataset)
        if make is True:
            os.makedirs(input_path, exist_ok=True)
    return input_path


# # append_trailer does not appear to be called anywhere
# def append_trailer(output_path, ipppssoot):  # pragma: no cover
#     """Fetch process log and append to trailer file
#     Note: copies trailer file from inputs directory
#     and copies to outputs directory prior to appending log
#     """
#     try:
#         tra1 = list(glob.glob(f"{output_path}/{ipppssoot.lower()}.tra"))
#         tra2 = list(glob.glob(f"{output_path}/{ipppssoot.lower()[0:5]}*.tra"))
#         if os.path.exists(tra1[0]):
#             trailer = tra1[0]
#         elif os.path.exists(tra2[0]):
#             trailer = tra2[0]
#         else:
#             log.info("Trailer file not found - skipping.")

#         log.info(f"Updating {trailer} with process log:")
#         proc_log = list(glob.glob(f"{os.getcwd()}/process.txt"))[0]
#         with open(trailer, "a") as tra:
#             with open(proc_log, "r") as proc:
#                 tra.write(proc.read())
#         log.info("Trailer file updated: ", trailer)
#     except IndexError:
#         log.info("Trailer file not found - skipping.")
#         return


def get_output_dir(output_uri):
    """Returns full path to output folder"""
    if output_uri.startswith("file"):
        output_dir = output_uri.split(":")[-1]
    elif output_uri.startswith("s3"):
        output_dir = os.path.abspath("outputs")
    return output_dir


def get_input_dir(input_uri):
    if input_uri.startswith("file"):
        input_dir = input_uri.split(":")[-1]
    else:
        input_dir = os.path.join(os.getcwd(), "inputs")
    return input_dir


# May need to be updated to include HAP output files
def find_output_files(ipppssoot):
    search_fits = f"{ipppssoot}/*.fits"
    search_tra = f"{ipppssoot}/*.tra"
    output_files = list(glob.glob(search_fits))
    output_files.extend(list(glob.glob(search_tra)))
    return output_files


def find_previews(dataset, output_files):
    search_prev = f"{dataset}/previews/*"
    output_files.extend(list(glob.glob(search_prev)))
    return output_files


def find_input_files(dataset):
    """If job fails (no outputs), tar the input files instead for debugging purposes."""
    search_inputs = f"{dataset}/*"
    file_list = list(glob.glob(search_inputs))
    return file_list


def make_tar(file_list, dataset):
    tar = dataset + ".tar.gz"
    log.info("Creating tarfile: ", tar)
    if os.path.exists(tar):
        os.remove(tar)  # clean up from prev attempts
    with tarfile.open(tar, "x:gz") as t:
        for f in file_list:
            print(os.path.basename(f))
            t.add(f)
    log.info("Tar successful: ", tar)
    tar_dest = os.path.join(dataset, tar)
    shutil.copy(tar, dataset)  # move tarfile to outputs/{ipst}
    os.remove(tar)
    return tar_dest


def upload_tar(tar, output_path):
    with sysexit.exit_on_exception(exit_codes.S3_UPLOAD_ERROR, "S3 tar upload of", tar, "to", output_path, "FAILED."):
        client = boto3.client("s3")
        parts = output_path[5:].split("/")
        bucket, prefix = parts[0], "/".join(parts[1:])
        objectname = prefix + "/" + os.path.basename(tar)
        log.info(f"Uploading: s3://{bucket}/{objectname}")
        if output_path.startswith("s3"):
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


def clean_up(file_list, dataset, dirs=None):
    print("\nCleaning up...")
    for f in file_list:
        try:
            os.remove(f)
        except FileNotFoundError:
            print(f"file {f} not found")
    if dirs is not None:
        for d in dirs:
            subdir = os.path.abspath(f"{dataset}/{d}")
            try:
                shutil.rmtree(subdir)
            except OSError:
                print(f"dir {subdir} not found")
    print("Done.")


def tar_outputs(dataset, input_uri, output_uri):
    working_dir = os.getcwd()
    output_path = process.get_output_path(output_uri, dataset)
    output_dir = get_output_dir(output_uri)
    os.chdir(output_dir)  # create tarfile with ipst/*fits (ipst is parent dir)
    output_files = find_output_files(dataset)
    if len(output_files) == 0:
        log.info("No output files found. Tarring inputs for debugging.")
        os.chdir(working_dir)
        input_dir = get_input_dir(input_uri)
        os.chdir(input_dir)
        file_list = find_input_files(dataset)
    else:
        file_list = find_previews(dataset, output_files)
    tar = make_tar(file_list, dataset)
    upload_tar(tar, output_path)
    clean_up(file_list, dataset, dirs=["previews", "env"])
    os.chdir(working_dir)
    return tar, file_list
