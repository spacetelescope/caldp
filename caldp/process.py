"""This module to defines classes and functions to do basic HST calibrations for
ACS, COS, STIS, and WFC3 instruments.   IPPPSSOOT IDs are processed and results are
output to an S3 bucket with IPPPSSOOT and batch name specific subdirectories.

1. Data corresponding to the IPPPSSOOT is downloaded from astroquery.
2. Calibration reference files corresponding to the data headers are defined and downloaded.
3. Level 1 and level 2 processing occurs as appropriate.
4. Outputs are copied to an S3 subdirectory for each IPPPSSOOT.

Notably,  previews are not currently computed here but rather in a seperate program run.
"""
import sys
import os
import shutil
import glob
import re
import subprocess
import tarfile

# -----------------------------------------------------------------------------

import boto3

from astropy.io import fits

try:  # pragma: no cover
    from drizzlepac.haputils.astroquery_utils import retrieve_observation
except ImportError:
    from drizzlepac.hlautils.astroquery_utils import retrieve_observation

from crds.bestrefs import bestrefs

from caldp import log
from caldp import messages
from caldp import exit_codes
from caldp import sysexit

# import caldp     (see track_versions)

# -----------------------------------------------------------------------------

IPPPSSOOT_RE = re.compile(r"^[IJLO][A-Z0-9]{8,8}$")

# Note: only ACS, COS, STIS, and WFC3 are initially supported
IPPPSSOOT_INSTR = {
    "J": "acs",
    "U": "wfpc2",
    "V": "hsp",
    "W": "wfpc",
    "X": "foc",
    "Y": "fos",
    "Z": "hrs",
    "E": "eng",
    "F": "fgs",
    "I": "wfc3",
    "N": "nicmos",
    "O": "stis",
    "L": "cos",
}

INSTRUMENTS = set(IPPPSSOOT_INSTR.values())


def get_instrument(ipppssoot):
    """Given an `ipppssoot` ID, return the corresponding instrument name.

    Parameters
    ----------
    ipppssoot : str
        HST-style dataset name,  'i' character identifies instrument:
            J  --  acs
            U  --  wfpc2
            I  --  wfc3
            O  --  stis
            L  --  cos

    Returns
    -------
    instrument : str
        Name of the instrument in lowercase corresponding to `ipppssoot`, e.g. 'acs'
    """
    if ipppssoot.lower() in INSTRUMENTS:
        return ipppssoot.lower()
    else:
        return IPPPSSOOT_INSTR.get(ipppssoot.upper()[0])


# -----------------------------------------------------------------------------
def get_output_path(output_uri, ipppssoot):
    """Given an `output_uri` string which nominally defines an S3 bucket and
    directory base path,  and an `ipppssoot` dataset name,  generate a full
    S3 output path (or directory) where outputs from processing `ipppssoot`
    should be stored.
    Parameters
    ----------
    output_uri : str
        A combination of S3 bucket and object directory prefix
    ipppssoot : str
        HST-style dataset name for which outputs will be stored.
    Returns
    -------
    object_path : str
        A fully specified S3 object, including bucket, directory, and filename,
        or a directory path.
    >>> get_output_path("s3://caldp-output-test/outputs", "j8cb010b0")
    's3://caldp-output-test/outputs/j8cb010b0'
    """
    # instrument_name = get_instrument(ipppssoot)
    if output_uri is None:
        return "none"
    elif output_uri.startswith("file"):
        prefix = output_uri.split(":")[-1]
        output_path = os.path.join(prefix, ipppssoot)  # 'outputs/obes03010'
    else:
        bucket = output_uri[5:].split("/")[0]
        output_path = f"s3://{bucket}/outputs/{ipppssoot}"

    return output_path


# -------------------------------------------------------------


def upload_filepath(ipppssoot, src_filepath, dest_filepath):
    """Given `filepath` to upload, copy it to `s3_filepath`.

    Parameters
    ----------
    filepath : str
       Local filesystem path to file to upload.
    s3_filepath : str
        Full S3 path to object to upload,  including the bucket prefix,
        e.g. s3://hstdp-batch-outputs/batch-1-2020-06-11T19-35-51/acs/J8CB010B0/process.txt

    Returns
    ------
    None
    """
    if dest_filepath.startswith("s3"):
        # make copies locally to be included in tarfile for s3
        output_dir = get_output_path("file:outputs", ipppssoot)
        os.makedirs(output_dir, exist_ok=True)
        local_outpath = os.path.join(output_dir, os.path.basename(dest_filepath))
        shutil.copy(src_filepath, local_outpath)
    else:
        os.makedirs(os.path.dirname(dest_filepath), exist_ok=True)
        shutil.copy(src_filepath, dest_filepath)


# -----------------------------------------------------------------------------


class InstrumentManager:
    """Abstract baseclass which is customized based on `instrument_name`,
    `download_suffixes`, `ignore_err_nums`, `stage1`, and `stage2` which
    must be redefined for each subclass.   Further customizations are
    applied as by overriding baseclass methods.

    Attributes
    ----------
    instrument_name : str
        (class) Name of the instrument supported by this manager in lower case.
    download_suffixes : list of str
        (class) Suffixes of files downloaded for each IPPPSSOOT as required by the
        astroquery `retrieve_observation` function.
    delete_endings : list of str
        (class) Endings of filenames to remove prior to previews or S3 uploads.
    ignore_err_nums : list of int
        (class) Nonzero calibration error codes which should be ignored. Different than
        CALDP codes in error_codes.py.
    stage1 : str
        (class) Program name for basic calibration and all association or unassociated files.
    stage2 : str
        (class) Program for follow-on  processing of calibrated association member files.

    ipppssoot  : str
        (instance) Name of dataset being processed
    output_uri : str
        (instance) Root output path,  e.g. s3://bucket/subdir/subdir/.../subdir
    input_uri : str
        (instance) root input path, or astroquery:// to download from MAST

    Notes
    -----
    InstrumentManager instances are lightweight and created for each `ipppssoot`.

    Methods
    -------
    __init__(ipppssoot, output_uri)
    raw_files(files)
    assoc_files(files)
    unassoc_files(files)
    divider(args, dash)
    main()
        Top level method orchestrating all activities.
    download()
        Downloads data files for `ipppssoot` from astroquery
    assign_bestrefs(input_files)
        Assigns CRDS best reference files to appropriate data files,  caches references.
    process(input_files)
        Applies stage1 and stage2 calibrations to associated an unassociated files as appropriate.
    run(cmd, *args)
        Joins `cmd` and `args` into a space separated single string,  executes as subprocess.
    output_files()
        Copies files to `output_uri` (and subdirs) unless `output_uri` is None or "none".
    """

    instrument_name = None  # abstract class
    download_suffixes = None  # abstract class
    delete_endings = []  # abstract class
    ignore_err_nums = []  # abstract class
    stage1 = None  # abstract class
    stage2 = None  # abstract class

    def __init__(self, ipppssoot, input_uri, output_uri):
        self.ipppssoot = ipppssoot
        self.input_uri = input_uri
        self.output_uri = output_uri

    # .............................................................

    def raw_files(self, files):
        """Return each name string in `files` with includes the substring '_raw'."""
        return [os.path.basename(f) for f in files if "_raw" in f]

    def assoc_files(self, files):
        """Return each name string in `files` which ends with '_asn.fits'."""
        return [os.path.basename(f) for f in files if f.endswith("_asn.fits")]

    def unassoc_files(self, files):  # can be overriden by subclasses
        """Overridable,  same as raw_files() by default."""
        return self.raw_files(files)

    # .............................................................

    def divider(self, *args, dash=">"):
        """Logs a standard divider made up of repeated `dash` characters as well as
        a message defined by the str() of each value in `args`.

        Parameters
        ----------
        args : list of values
            Values filtered through str() and joined with a single space into a message.
        dash : str
            Separator character repeated to create the divider string

        Returns
        -------
        None
        """
        assert len(dash) == 1
        msg = " ".join([str(a) for a in args])
        dashes = 100 - len(msg) - 2
        log.info(dash * 80)
        log.info(dash * 5, self.ipppssoot, msg, dash * (dashes - 6 - len(self.ipppssoot) - len(msg) - 1))

    def run(self, exit_code, cmd, *args):
        """Run the subprocess string `cmd`,  appending any extra values
        defined by `args`.

        Parameters
        ----------
        cmd : str
            A multi-word subprocess command string
        args : tuple of str
            Extra single-word parameters to append to `cmd`,  nominally
            filenames or switches.

        Returns
        -------
        None

        Notes
        -----
        Logs executed command tuple.

        Checks subprocess error status against class attribute `ignore_err_nums`
        to ignore instrument-specific error codes.

        Issues an ERROR message and exits the program for any other non-zero
        value.
        """
        cmd = tuple(cmd.split()) + args  # Handle stage values with switches.
        self.divider("Running:", cmd)
        with sysexit.exit_on_exception(exit_code, self.ipppssoot, "Command:", repr(cmd)):
            err = subprocess.call(cmd)
            if err in self.ignore_err_nums:
                log.info("Ignoring error status =", err)
            elif err:
                raise RuntimeError(f"Subprocess returned with non-zero status = {err}")

    def run_stage1(self, *args):
        return self.run(exit_codes.STAGE1_ERROR, self.stage1, *args)

    def run_stage2(self, *args):
        return self.run(exit_codes.STAGE2_ERROR, self.stage2, *args)

    # .............................................................

    def main(self):
        """Perform all processing steps for basic calibration processing:
        1. Download uncalibrated data
        2. Assign bestrefs (and potentially download reference files)
        3. Perform stage1 and stage2 CAL processing
        4. Optionally remove files prior to S3 uploads or previews
        4. Copy outputs to S3
        5. Issues start and stop dividers
        """
        self.divider("Started processing for", self.instrument_name, self.ipppssoot)

        # we'll need to move around a couple of times to get the cal code and local file movements working
        orig_wd = os.getcwd()
        input_path = self.get_input_path()
        if self.input_uri.startswith("astroquery"):
            os.chdir(input_path)
            input_files = self.download()
        elif self.input_uri.startswith("file"):
            input_files = self.find_input_files()
            # need to be in the same place as asn's for association processing
            os.chdir(input_path)
        elif self.input_uri.startswith("s3"):
            os.chdir(input_path)
            input_files = self.get_objects(input_path)
        else:
            raise ValueError("input_uri should start with s3, astroquery or file")

        self.set_env_vars()

        self.assign_bestrefs(input_files)

        self.process(input_files)

        # chdir back for relative output path
        os.chdir(orig_wd)

        self.output_files()

        # clarification; file cleaning-up happens in create_previews

        self.divider("Completed processing for", self.instrument_name, self.ipppssoot)

    # -----------------------------------------------------------------------------

    def get_input_path(self):
        """Creates subfolder in current directory prior to downloading files.
        Returns path to subdirectory (named using ipppssoot).
        For file:, simply returns path to inputs.
        """
        cwd = os.getcwd()
        if self.input_uri.startswith("file"):
            input_path = self.input_uri.split(":")[-1]
        else:
            input_path = os.path.join(cwd, "inputs", self.ipppssoot)
            os.makedirs(input_path, exist_ok=True)
        return input_path

    def get_objects(self, input_path):
        """Called if input_uri starts with `s3`
        For S3 Inputs: Downloads compressed ipppssoot (tar.gz) files,
        Extracts, then saves file paths to a sorted list.
        Returns sorted list of file paths (`input_files`)
        """
        key = self.ipppssoot + ".tar.gz"
        s3_path = self.input_uri.replace("s3://", "").split("/")
        bucket, prefix = s3_path[0], "/".join(s3_path[1:])
        if len(prefix) == 0:
            obj = key
        else:  # remove trailing slash from prefix if there is one
            obj = prefix.strip("/") + "/" + key
        self.divider(f"Retrieving tarfile: s3://{bucket}/{obj}")
        with sysexit.exit_on_exception(
            exit_codes.S3_DOWNLOAD_ERROR, f"Failed downloading or extracting s3://{bucket}/{obj}"
        ):
            client = boto3.client("s3")
            with open(key, "wb") as f:
                client.download_fileobj(bucket, obj, f)  # 'odfa0120.tar.gz'

        with sysexit.exit_on_exception(exit_codes.INPUT_TAR_FILE_ERROR, "Failed extracting inputs from", key):
            self.divider(f"Extracting files from {key}")
            with tarfile.open(key, "r:gz") as tar_ref:
                tar_ref.extractall()
                # then delete tars
        os.remove(key)
        self.divider("Gathering fits files for calibration")
        search_fits = f"{input_path}/{self.ipppssoot.lower()[0:5]}*.fits"
        self.divider("Finding input data using:", repr(search_fits))
        files = glob.glob(search_fits)
        return list(sorted(files))

    def download(self):
        """Called if input_uri starts is `astroquery`
        Download any data files for the `ipppssoot`,  issuing start and
        stop divider messages.

        Returns
        -------
        filepaths : sorted list
            Local file system paths of files which were downloaded for `ipppssoot`,
            some of which will be selected for calibration processing.
        """
        with sysexit.exit_on_exception(
            exit_codes.ASTROQUERY_ERROR, "Astroquery exception downloading suffixes:", self.download_suffixes
        ):
            self.divider("Retrieving data files for:", self.download_suffixes)
            files = retrieve_observation(self.ipppssoot, suffix=self.download_suffixes)
            self.divider("Download data complete.")
            return list(sorted([os.path.abspath(f) for f in files]))

    def find_input_files(self):
        """Scrape the input_uri for the needed input_files.
        Called if input_uri starts with `file:`
        Returns
        -------
        filepaths : sorted list
            Local file system paths of files which were found for `ipppssoot`,
            some of which will be selected for calibration processing.
        """
        test_path = self.input_uri.split(":")[-1]
        if os.path.isdir(test_path):
            base_path = os.path.abspath(test_path)
        elif os.path.isdir(os.path.join(os.getcwd(), test_path)):
            base_path = os.path.join(os.getcwd(), test_path)
        else:
            raise ValueError(f"input path {test_path} does not exist")
        # check for tarred inputs
        cwd = os.getcwd()
        search_tar = f"{base_path}/{self.ipppssoot.lower()[0:5]}*.tar.gz"
        tar_files = glob.glob(search_tar)
        with sysexit.exit_on_exception(exit_codes.INPUT_TAR_FILE_ERROR, "Failed extracting inputs from", tar_files):
            if len(tar_files) == 0:
                raise RuntimeError(f"No input tar files for: {repr(search_tar)}")
            elif len(tar_files) == 1:
                log.info("Extracting inputs from: ", tar_files)
                os.chdir(base_path)
                with tarfile.open(tar_files[0], "r:gz") as tar_ref:
                    tar_ref.extractall()
            else:
                raise RuntimeError(f"Too many tar files for: {repr(search_tar)} = {tar_files}")
        os.chdir(cwd)
        # get input files
        search_str = f"{base_path}/{self.ipppssoot.lower()[0:5]}*.fits"
        self.divider("Finding input data using:", repr(search_str))
        # find the base path to the files
        files = glob.glob(search_str)
        return list(sorted(files))

    def find_output_files(self):
        """Scrape the input_uri for the needed output_files, to be run after calibration is finished.

        Returns
        -------
        filepaths : sorted list
            Local file system paths of files which were found for `ipppssoot`,
            post-calibration
        """
        # find the base path to the files
        if self.input_uri.startswith("file"):
            test_path = self.input_uri.split(":")[-1]
            if os.path.isdir(test_path):
                base_path = os.path.abspath(test_path)
            elif os.path.isdir(os.path.join(os.getcwd(), test_path)):
                base_path = os.path.join(os.getcwd(), test_path)
            else:
                raise ValueError(f"output path {test_path} does not exist")
            search_fits = f"{base_path}/{self.ipppssoot.lower()[0:5]}*.fits"
            # trailer files
            search_tra = f"{base_path}/{self.ipppssoot.lower()[0:5]}*.tra"
            # env file
            search_env = f"{base_path}/{self.ipppssoot.lower()}_cal_env.txt"

        else:
            base_path = os.getcwd()
            subfolder = os.path.join(base_path, "inputs", self.ipppssoot)
            search_fits = f"{subfolder}/{self.ipppssoot.lower()[0:5]}*.fits"
            search_tra = f"{subfolder}/{self.ipppssoot.lower()[0:5]}*.tra"
            search_env = f"{subfolder}/{self.ipppssoot.lower()}_cal_env.txt"

        self.divider("Finding output data for:", repr(search_fits))
        files = glob.glob(search_fits)

        self.divider("Finding output trailers for:", repr(search_tra))
        files.extend(glob.glob(search_tra))

        self.divider("Finding output cal env file for:", repr(search_env))
        files.extend(glob.glob(search_env))

        return list(sorted(files))

    def assign_bestrefs(self, files):
        """Assign best references to dataset `files`,  updating their header keywords
        and downloading any references which aren't already cached.  Uses CRDS.

        Parameters
        ----------
        files : list of str
            Paths of files which should be run through CRDS to assign best references
            to their file headers,  also ensuring the references are available in the
            CRDS cache,  downloading them if required.

        Returns
        -------
        None
        """
        with sysexit.exit_on_exception(exit_codes.BESTREFS_ERROR, "Failed computing or downloading reference files."):
            self.divider("Computing bestrefs and downloading references.", files)
            bestrefs_files = self.raw_files(files)
            # Only sync reference files if the cache is read/write.
            bestrefs.assign_bestrefs(
                bestrefs_files,
                context=os.environ.get("CRDS_CONTEXT", default=None),
                sync_references=os.environ.get("CRDS_READONLY_CACHE", "0") != "1",
            )
            self.divider("Bestrefs complete.")

    def set_env_vars(self):
        """looks for an ipppssoot_cal_env.txt file and sets the key=value
        pairs in the file in os.environ for the calibration code
        """
        env_file = f"{self.ipppssoot}_cal_env.txt"
        if os.path.isfile(env_file):
            self.divider(f"processing env file {env_file}")
            with open(env_file, "r") as f:
                for line in f.readlines():
                    try:
                        key, value = line.split("=")
                    except ValueError:
                        log.info(f"{line} is not a valid key=value pair")
                        continue
                    os.environ[key.strip()] = value.strip()
                    log.info(f"setting {key}={value} in processing env")
        return

    def process(self, files):
        """Runs each filepath in `files` through calibration processing.   Association
        files are run through both stage1 and stage2 calibration programs,
        Unassociated files are run through only the stage1 program.  Version keywords
        are set for all raw files.

        Parameters
        ----------
        files : list of str
            Filepaths of files to filter and calibrate as appropriate.

        Returns
        -------
        None
        """
        self.track_versions(files)
        astrodriz_params = ["-n", "1"]
        assoc = self.assoc_files(files)
        if assoc:
            self.run_stage1(*assoc)
            if self.stage2:
                args = astrodriz_params + assoc
                self.run_stage2(*args)
            return
        unassoc = self.unassoc_files(files)
        if unassoc:
            self.run_stage1(*unassoc)
            if self.stage2:
                args = astrodriz_params + unassoc
                self.run_stage2(*args)
            return

    def output_files(self):
        """Selects files from the current working directory and uploads them
        to the `output_uri`.   If `output_uri` is None or "none",  returns
        without copying files.

        Returns
        -------
        None
        """
        outputs = self.find_output_files()
        delete = [output for output in outputs if output.endswith(tuple(self.delete_endings))]
        if delete:
            self.divider("Deleting files:", delete)
            for filename in delete:
                os.remove(filename)
            outputs = self.find_output_files()  # get again
        self.divider(f"Saving {len(outputs)} outputs")
        if self.output_uri is None or self.output_uri.startswith("none"):
            return
        output_path = get_output_path(self.output_uri, self.ipppssoot)
        for filepath in outputs:
            # move the env file out of the way for on-premise archiving/cataloging
            if filepath.endswith("_cal_env.txt"):
                output_filename = f"{output_path}/env/{os.path.basename(filepath)}"
            else:
                output_filename = f"{output_path}/{os.path.basename(filepath)}"
            log.info(f"\t{output_filename}")
            upload_filepath(self.ipppssoot, filepath, output_filename)
        self.divider("Saving outputs complete.")

    def track_versions(self, files, apply_to="_raw"):
        """Add version keywords to raw_files(files)."""

        csys_ver = os.environ.get("CSYS_VER", "UNDEFINED")
        awsdpver = os.environ.get("AWSDPVER", "UNDEFINED")
        awsysver = os.environ.get("AWSYSVER", "UNDEFINED")
        for filename in self.raw_files(files):
            if apply_to in filename:
                fits.setval(filename, "CSYS_VER", value=csys_ver)
                fits.setval(filename, "AWSDPVER", value=awsdpver)
                fits.setval(filename, "AWSYSVER", value=awsysver)


# -----------------------------------------------------------------------------


class AcsManager(InstrumentManager):
    """Manages calibration for one ACS IPPPSSOOT."""

    instrument_name = "acs"
    download_suffixes = ["ASN", "RAW"]
    stage1 = "calacs.e"
    stage2 = "runastrodriz"


class Wfc3Manager(InstrumentManager):
    """Manages calibration for one WFC3 IPPPSSOOT."""

    instrument_name = "wfc3"
    download_suffixes = ["ASN", "RAW"]
    stage1 = "calwf3.e"
    stage2 = "runastrodriz"


class CosManager(InstrumentManager):
    """Manages calibration for one COS IPPPSSOOT."""

    instrument_name = "cos"
    download_suffixes = [
        "ASN",
        "RAW",
        "EPC",
        "SPT",
        "RAWACCUM",
        "RAWACCUM_A",
        "RAWACCUM_B",
        "RAWACQ",
        "RAWTAG",
        "RAWTAG_A",
        "RAWTAG_B",
        "PHA_A",
        "PHA_B",
    ]
    stage1 = "calcos"
    stage2 = None
    ignore_err_nums = [5]  # Ignore calcos errors from RAWACQ

    def unassoc_files(self, files):
        """Returns only the first file returned by raw_files()."""
        return super().raw_files(files)[:1]  # return only first file

    def process(self, files):
        """Set keyword RANDSEED=1 in each raw file and process normally."""
        for filename in self.raw_files(files):
            fits.setval(filename, "RANDSEED", value=1)
        return super().process(files)


class StisManager(InstrumentManager):
    """Manages calibration for one"""

    instrument_name = "stis"
    download_suffixes = ["ASN", "RAW", "EPC", "TAG", "WAV"]
    delete_endings = ["_epc.fits"]
    stage1 = "cs0.e -tv"
    stage2 = None

    def process(self, files):
        """Apply STIS calibrations to selected _raw.fits or _wav.fits `files`.

        Parameters
        ----------
        files : list of str
            Filepaths containing raw and wav files to calibrate.

        Returns
        -------
        None
        """
        raw = [os.path.basename(f) for f in files if f.endswith("_raw.fits")]
        wav = [os.path.basename(f) for f in files if f.endswith("_wav.fits")]
        if raw:
            self.track_versions(files, "_raw")
            self.run_stage1(*raw)
        else:
            self.track_versions(files, "_wav")
            self.run_stage1(*wav)

    def raw_files(self, files):
        """Returns only '_raw.fits', '_wav.fits', or '_tag.fits' members of `files`."""
        return [os.path.basename(f) for f in files if f.endswith(("_raw.fits", "_wav.fits", "_tag.fits"))]


# ............................................................................

MANAGERS = {"acs": AcsManager, "cos": CosManager, "stis": StisManager, "wfc3": Wfc3Manager}


def get_instrument_manager(ipppssoot, input_uri, output_uri):
    """Given and `ipppssoot`, `input_uri`, and `output_uri`,  determine
    the appropriate instrument manager from the `ipppssoot`
    and construct and return it.

    Parameters
    ----------
    ipppssoot : str
        The HST dataset name to be processed.
    output_uri : str
        The base path to which outputs will be copied, nominally S3://bucket/subdir/.../subdir
    input_uri : str
        either a local directory (path in the container) or astroquery to download from MAST

    Returns
    -------
    instrument_manager : InstrumentManager subclass
        The instrument-specific InstrumentManager subclass instance appropriate for
        processing dataset name `ipppssoot`.
    """
    instrument = get_instrument(ipppssoot)
    manager = MANAGERS[instrument](ipppssoot, input_uri, output_uri)
    return manager


# -----------------------------------------------------------------------------


def process(ipppssoot, input_uri, output_uri):
    """Given an `ipppssoot`, `input_uri`, and `output_uri` where products should be stored,
    perform all required processing steps for the `ipppssoot` and store all
    products to `output_uri`.

    Parameters
    ----------
    ipppssoot : str
        The HST dataset name to be processed.
    output_uri : str
        The base path to which outputs will be copied, nominally S3://bucket/subdir/.../subdir
    input_uri : str
        either a local directory (path in the container) or astroquery to download from MAST

    Returns
    -------
    None
    """
    process_log = log.CaldpLogger(enable_console=False, log_file="process.txt")

    if output_uri is None:
        output_uri, output_path = messages.path_finder(input_uri, output_uri, ipppssoot)
    output_path = get_output_path(output_uri, ipppssoot)

    msg = messages.Messages(output_uri, output_path, ipppssoot)
    msg.init()
    msg.process_message()  # processing-ipst

    manager = get_instrument_manager(ipppssoot, input_uri, output_uri)
    manager.main()

    del process_log


def download_inputs(ipppssoot, input_uri, output_uri, make_env=False):
    """This function sets up file inputs for CALDP based on downloads from
    astroquery to support testing the file based input mode.  The files for
     `ipppssoot` normally downloaded from astroquery: are downloaded and placed
    in the directory defined by `input_uri`. This function uses a parameter set
    identical to `process.process()` to ease construction of test cases and
    to fully construct an appropriate instrument manager based on the `ipppssoot`.
    """
    manager = get_instrument_manager(ipppssoot, input_uri, output_uri)
    manager.download()
    input_files = list(glob.glob("*.fits"))
    if make_env:  # ensures test coverage for InstrumentManager.set_env_vars()
        with open(f"{ipppssoot}_cal_env.txt", "w") as f:
            f.write("BadKey|ValuePair\n")
            f.write("GoodKey=ValuePair\n")
    tar = ipppssoot + ".tar.gz"
    with tarfile.open(tar, "x:gz") as t:
        for f in input_files:
            t.add(f)


# -----------------------------------------------------------------------------


def process_ipppssoots(ipppssoots, input_uri=None, output_uri=None):
    """Given a list of `ipppssoots`, and `input_uri`,  and an `output_uri` defining
    the base path at which to store outputs,  calibrate data corresponding to
    each of the ipppssoots,  including any association members.

    Parameters
    ----------
    ipppssoots : list of str
        HST dataset names to process, e.g.  [ 'J8CB010B0', ...]

    output_uri:  str
        S3 bucket and object prefix
        e.g. 's3://hstdp-batch-outputs/batch-1-2020-06-11T19-35-51'
        or local path within the container
        e.g. 'file:/home/developer/caldp-outputs

    input_uri: str
        either s3://bucket/tarfile or astroquery:// or file:/path/to/files

    Notes
    -----
    The `output_uri` is extended by the instrument and ipppssoot as well as
    any filename for each output file generated.   For a single ipppssoot,
    the path extension is the same for each output file.

    e.g. 's3://hstdp-batch-outputs/batch-1-2020-06-11T19-35-51/acs/J8CB010B0/j8cb01u3q_raw.fits'

    Returns
    -------
    None
    """
    for ipppssoot in ipppssoots:
        process(ipppssoot, input_uri, output_uri)


# -----------------------------------------------------------------------------


def test():
    from caldp import process
    import doctest

    return doctest.testmod(process)


# -----------------------------------------------------------------------------


def main(argv):
    """Top level function, process args <input_uri> <output_uri>  <ipppssoot's...>"""
    input_uri = argv[1]
    output_uri = argv[2]
    ipppssoots = argv[3:]
    if input_uri.endswith("/"):
        input_uri.rstrip("/")
    if output_uri.lower().startswith("none"):
        output_uri = None
    process_ipppssoots(ipppssoots, input_uri, output_uri)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("usage:  process.py <input_uri>  <output_uri>  <ipppssoot's...>")
        sys.exit(exit_codes.CMDLINE_ERROR)
    with sysexit.exit_receiver():
        main(sys.argv)
