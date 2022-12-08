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


def find_output_files(dataset):
    if process.IPPPSSOOT_RE.match(dataset):
        search_fits = f"{dataset}/*.fits"
        search_tra = f"{dataset}/*.tra"
        output_files = list(glob.glob(search_fits))
        output_files.extend(list(glob.glob(search_tra)))
        return output_files
    elif process.SVM_RE.match(dataset) and dataset.split("_")[0] in list(process.SVM_INSTR.keys()):
        ipppss = process.get_svm_obs_set(dataset)
        search_fits = f"{dataset}/hst_*{ipppss}*.fits"
        search_txt = f"{dataset}/hst_*{ipppss}*.txt"
        search_ecsv = f"{dataset}/hst_*{ipppss}*.ecsv"
        search_manifest = f"{dataset}/{dataset}_manifest.txt"
        search_log = f"{dataset}/astrodrizzle.log"
        output_files = list(glob.glob(search_fits))
        output_files.extend(list(glob.glob(search_txt)))
        output_files.extend(list(glob.glob(search_ecsv)))
        output_files.extend(list(glob.glob(search_manifest)))
        output_files.extend(list(glob.glob(search_log)))
        return output_files
    elif process.MVM_RE.match(dataset):
        search_fits = f"{dataset}/hst_{dataset}*.fits"
        search_txt = f"{dataset}/hst_{dataset}*.txt"
        search_manifest = f"{dataset}/{dataset}_manifest.txt"
        output_files = list(glob.glob(search_fits))
        output_files.extend(list(glob.glob(search_txt)))
        output_files.extend(list(glob.glob(search_manifest)))
        return output_files
    else:
        raise ValueError("Invalid dataset name {dataset}, dataset must be an ipppssoot, SVM, or MVM dataset")


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
