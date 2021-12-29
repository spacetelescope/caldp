"""This module creates preview images from reprocessing output data."""

import argparse
import os
import sys
import subprocess
import logging
import glob
import shutil
import boto3
from astropy.io import fits

from caldp import log
from caldp import process
from caldp import file_ops
from caldp import messages
from caldp import sysexit
from caldp import exit_codes

# -------------------------------------------------------------------------------------------------------

LOGGER = logging.getLogger(__name__)

AUTOSCALE = 99.5

OUTPUT_FORMATS = [("_thumb", 128), ("", -1)]


# -------------------------------------------------------------------------------------------------------


def generate_image_preview(input_path, output_path, size):
    cmd = [
        "fitscut",
        "--all",
        "--jpg",
        f"--autoscale={AUTOSCALE}",
        "--asinh-scale",
        f"--output-size={size}",
        "--badpix",
        input_path,
    ]

    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    if process.returncode > 0:
        LOGGER.error("fitscut failed for %s with status %s: %s", input_path, process.returncode, stderr)
        raise RuntimeError()

    with open(output_path, "wb") as f:
        f.write(stdout)


def generate_image_previews(input_path, input_dir, filename_base):
    for suffix, size in OUTPUT_FORMATS:
        output_path = os.path.join(input_dir, f"{filename_base}{suffix}.jpg")
        try:
            generate_image_preview(input_path, output_path, size)
        except Exception:
            log.info("Preview file (imaging) not generated for", input_path, "with size", size)


def generate_spectral_previews(input_path, input_dir):
    cmd = ["make_hst_spec_previews", "-v", "-t", "png", "fits", "-o", input_dir, input_path]
    err = subprocess.call(cmd)
    if err:
        LOGGER.exception(f"Preview file not generated for {input_path}")
        return []


def generate_previews(input_path, input_dir, filename_base):
    with fits.open(input_path) as hdul:
        naxis = hdul[1].header["NAXIS"]
        ext = hdul[1].header["XTENSION"]
        extname = hdul[1].header["EXTNAME"].strip()
        try:
            instr_char = hdul[1].header["INSTRUME"].strip()[0]
        except Exception:
            instr_char = filename_base[0]
        instr_char = instr_char.lower()

    if naxis == 2 and ext == "BINTABLE" and extname != "ASN":
        print("Generating spectral previews...")
        return generate_spectral_previews(input_path, input_dir)
    elif naxis >= 2 and ext == "IMAGE" and instr_char not in ["l", "o"]:
        print("Generating image previews...")
        return generate_image_previews(input_path, input_dir, filename_base)
    else:
        log.warning("Unable to determine FITS file type")
        return []


def get_inputs(ipppssoot, input_dir):
    search_fits = f"{input_dir}/{ipppssoot.lower()[0:5]}*.fits"
    inputs = glob.glob(search_fits)
    return list(sorted(inputs))


def get_suffix(instr):
    if instr == "stis":
        req_sfx = ["x1d", "sx1"]
    elif instr == "cos":
        req_sfx = ["x1d", "x1dsum"] + ["x1dsum" + str(x) for x in range(1, 5)]
    elif instr == "acs":
        req_sfx = ["crj", "drc", "drz", "raw", "flc", "flt"]
    elif instr == "wfc3":
        req_sfx = [
            "drc",
            "drz",
            "flc",
            "flt",
            "ima",
            "raw",
        ]
    else:
        req_sfx = ""
    return req_sfx


def get_preview_inputs(instr, input_paths):
    req_sfx = get_suffix(instr)
    preview_inputs = []
    if req_sfx:
        for input_path in input_paths:
            file_sfx = os.path.basename(input_path).split(".")[0].split("_")[-1]
            if file_sfx in req_sfx:
                preview_inputs.append(input_path)
            else:
                continue
    else:
        preview_inputs = input_paths
    return preview_inputs


def get_previews(input_dir):
    png_search = f"{input_dir}/*.png"
    jpg_search = f"{input_dir}/*.jpg"
    prev_search = f"{input_dir}/*_prev.fits"
    preview_files = glob.glob(png_search)
    preview_files.extend(glob.glob(jpg_search))
    preview_files.extend(glob.glob(prev_search))
    return list(sorted(preview_files))


def create_previews(input_dir, preview_inputs):
    """Generates previews based on s3 downloads
    Returns a list of file paths to previews
    """
    log.info("Processing", len(preview_inputs), "FITS file(s) from ", input_dir)
    # Generate previews to local preview folder inside ipppssoot folder
    for input_path in preview_inputs:
        log.info("Generating previews for", input_path)
        filename_base = os.path.basename(input_path).split(".")[0]
        generate_previews(input_path, input_dir, filename_base)
    # list of full paths to preview files
    previews = get_previews(input_dir)
    log.info("Generated", len(previews), "preview files")
    return previews


def upload_previews(previews, output_path):
    """Given `previews` list to upload, copy it to `output_uri_prefix`.
    previews : List of local preview filepaths to upload
       ['./odfa01030/previews/x1d_thumb.png','./odfa01030/previews/x1d.png' ]
    output_uri_prefix : Full path to object to upload including the bucket prefix
        s3://hstdp-batch-outputs/data/stis/odfa01030/previews/
    """
    client = boto3.client("s3")
    splits = output_path[5:].split("/")
    bucket, path = splits[0], "/".join(splits[1:])
    for preview in previews:
        preview_file = os.path.basename(preview)
        objectname = path + "/" + preview_file
        log.info(f"\t{output_path}/{preview_file}")
        with open(preview, "rb") as f:
            client.upload_fileobj(f, bucket, objectname)


def copy_previews(previews, output_path):
    for filepath in previews:
        preview_file = os.path.join(output_path, os.path.basename(filepath))
        shutil.copy(filepath, preview_file)
        log.info(f"\t{preview_file}")
    os.listdir(output_path)


def main(ipppssoot, input_uri_prefix, output_uri_prefix):
    """Generates previews based on input and output directories
    according to specified args
    """
    output_path = messages.get_local_outpath(output_uri_prefix, ipppssoot)
    msg = messages.Messages(output_uri_prefix, output_path, ipppssoot)
    msg.preview_message()  # processing
    logger = log.CaldpLogger(enable_console=False, log_file="preview.txt")
    input_dir = file_ops.get_input_path(input_uri_prefix, ipppssoot)
    # append process.txt to trailer file
    # file_ops.append_trailer(input_dir, output_path, ipppssoot)
    input_paths = get_inputs(ipppssoot, input_dir)
    instr = process.get_instrument(ipppssoot)
    preview_inputs = get_preview_inputs(instr, input_paths)
    # create previews
    previews = create_previews(input_dir, preview_inputs)
    # upload/copy previews
    log.info("Saving previews...")
    if output_uri_prefix.startswith("s3"):
        preview_output = process.get_output_path("file:outputs", ipppssoot) + "/previews"
        os.makedirs(preview_output, exist_ok=True)
        copy_previews(previews, preview_output)
        log.info("Preparing files for s3 upload...")
        file_ops.tar_outputs(ipppssoot, input_uri_prefix, output_uri_prefix)
    elif output_uri_prefix.startswith("file"):
        preview_output = process.get_output_path(output_uri_prefix, ipppssoot) + "/previews"
        os.makedirs(preview_output, exist_ok=True)
        copy_previews(previews, preview_output)
    else:
        return
    del logger


def parse_args():
    parser = argparse.ArgumentParser(description="Create image and spectral previews")
    parser.add_argument("input_uri_prefix", help="s3 or local directory containing FITS images")
    parser.add_argument("output_uri_prefix", help="S3 URI prefix for writing previews")
    parser.add_argument("ipppssoot", help="IPPPSSOOT for instrument data")
    return parser.parse_args()


def cmdline():
    args = parse_args()
    if args.output_uri_prefix.lower().startswith("none"):
        if args.input_uri_prefix.startswith("file"):
            output_uri_prefix = args.input_uri_prefix
        else:
            output_uri_prefix = os.path.join(os.getcwd(), args.ipppssoot)
    else:
        output_uri_prefix = args.output_uri_prefix

    main(args.ipppssoot, args.input_uri_prefix, output_uri_prefix)


if __name__ == "__main__":
    with sysexit.exit_on_exception():
        cmdline()
    sys.exit(exit_codes.SUCCESS)
