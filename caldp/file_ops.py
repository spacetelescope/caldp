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


def get_input_path(input_uri, ipppssoot, make=False):
    """Fetches the path to input files"""
    cwd = os.getcwd()
    if input_uri.startswith("file"):
        input_path = input_uri.split(":")[-1]
    else:
        input_path = os.path.join(cwd, "inputs", ipppssoot)
        if make is True:
            os.makedirs(input_path, exist_ok=True)
    return input_path


def append_trailer(input_path, output_path, ipppssoot):  # pragma: no cover
    """Fetch process log and append to trailer file
    Note: copies trailer file from inputs directory
    and copies to outputs directory prior to appending log
    """
    try:
        tra1 = list(glob.glob(f"{output_path}/{ipppssoot.lower()}.tra"))
        tra2 = list(glob.glob(f"{output_path}/{ipppssoot.lower()[0:5]}*.tra"))
        if os.path.exists(tra1[0]):
            trailer = tra1[0]
        elif os.path.exists(tra2[0]):
            trailer = tra2[0]
        else:
            log.info("Trailer file not found - skipping.")

        log.info(f"Updating {trailer} with process log:")
        proc_log = list(glob.glob(f"{os.getcwd()}/process.txt"))[0]
        with open(trailer, "a") as tra:
            with open(proc_log, "r") as proc:
                tra.write(proc.read())
        log.info("Trailer file updated: ", trailer)
    except IndexError:
        log.info("Trailer file not found - skipping.")
        return


def get_output_dir(output_uri):
    """Returns full path to output folder """
    if output_uri.startswith("file"):
        output_dir = output_uri.split(":")[-1]
    elif output_uri.startswith("s3"):
        output_dir = os.path.abspath("outputs")
    return output_dir


def find_files(ipppssoot):
    search_fits = f"{ipppssoot}/*.fits"
    search_tra = f"{ipppssoot}/*.tra"
    search_prev = f"{ipppssoot}/previews/*"
    file_list = list(glob.glob(search_fits))
    file_list.extend(list(glob.glob(search_tra)))
    file_list.extend(list(glob.glob(search_prev)))
    return file_list


def make_tar(file_list, ipppssoot):
    tar = ipppssoot + ".tar.gz"
    log.info("Creating tarfile: ", tar)
    if os.path.exists(tar):
        os.remove(tar)  # clean up from prev attempts
    with tarfile.open(tar, "x:gz") as t:
        for f in file_list:
            t.add(f)
    log.info("Tar successful: ", tar)
    tar_dest = os.path.join(ipppssoot, tar)
    shutil.copy(tar, ipppssoot)  # move tarfile to outputs/{ipst}
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
    working_dir = os.getcwd()
    output_path = process.get_output_path(output_uri, ipppssoot)
    output_dir = get_output_dir(output_uri)
    os.chdir(output_dir)  # create tarfile with ipst/*fits (ipst is parent dir)
    file_list = find_files(ipppssoot)
    tar = make_tar(file_list, ipppssoot)
    upload_tar(tar, output_path)
    os.chdir(working_dir)
    # clean_up(file_list, ipppssoot, dirs=["previews"])
    if output_uri.startswith("file"):  # test cov only
        return tar, file_list  # , local_outpath
