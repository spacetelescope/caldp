import sys
import os
import shutil
import glob
import re
import subprocess
import tarfile
import boto3

from caldp import process

def get_path(output_uri, ipppssoot):
    output_path = process.get_output_path(output_uri, ipppssoot)
    if output_uri.startswith('s3'):
        file_path = process.get_output_path("file:outputs", ipppssoot)
    else:
        file_path = output_path
    return output_path, file_path

def find_files(file_path):
    data_sfx = ['fits', 'tra']
    img_sfx = ['png', 'jpg', 'fits']
    file_list = []
    for _, _, files in os.walk(file_path):
        for f in files:
            file_sfx = f.split('.')[-1]
            if file_sfx in data_sfx:
                file_list.append(f)
            elif file_sfx in img_sfx:
                p = os.path.join("previews", f)
                file_list.append(p)
            else:
                continue
    return file_list


def make_tar(file_list, file_path, ipppssoot):
    working_dir = os.getcwd()
    os.chdir(file_path)
    tar_name = ipppssoot + ".tar.gz"
    print("Creating tarfile: ", tar_name)
    if os.path.exists(tar_name):
        os.remove(tar_name) # clean up from prev attempts
    with tarfile.open(tar_name, "x:gz") as tar:
        for f in file_list:
            tar.add(f)
    tar_dest = os.path.join(working_dir, tar_name)
    if os.path.exists(tar_dest):
        os.remove(tar_dest) # clean up from prev attempts
    shutil.copy(tar_name, tar_dest)
    print("Tar successful: ", tar_dest)
    print("Cleaning up...")
    os.remove(tar_name)
    print("Done.")
    os.chdir(working_dir)
    return tar_dest


def upload_tar(tar, output_path):
    client = boto3.client("s3")
    parts = output_path[5:].split('/')
    bucket, prefix = parts[0], '/'.join(parts[1:])
    objectname = prefix + os.path.basename(tar)
    print("Uploading: ", objectname)
    with open(tar, "rb") as f:
        client.upload_fileobj(f, bucket, objectname)


def main(ipppssoot, input_uri, output_uri):
    output_path, file_path = get_path(output_uri, ipppssoot)
    file_list = find_files(file_path)
    tar = make_tar(file_list, file_path, ipppssoot)
    if output_uri.startswith('s3'):
        upload_tar(tar, output_path)


def run(argv):
    """Top level function, process args <input_uri> <output_uri>  <ipppssoot's...>"""
    input_uri = argv[1]
    output_uri = argv[2]
    ipppssoot = argv[3]
    main(ipppssoot, input_uri, output_uri)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("usage:  process.py <input_uri>  <output_uri>  <ipppssoot's...>")
        sys.exit(1)
    run(sys.argv)
