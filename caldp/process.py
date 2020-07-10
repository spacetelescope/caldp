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
import glob
import re
import subprocess

# -----------------------------------------------------------------------------

import boto3

from astropy.io import fits
from drizzlepac.hlautils.astroquery_utils import retrieve_observation

from crds.bestrefs import bestrefs

from . import log

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


def get_output_path(output_uri,  ipppssoot):
    """Given an `output_uri` string which nominally defines an S3 bucket and
    directory base path,  and an `ipppssoot` dataset name,  generate a full
    S3 output path where outputs from processing `ipppssoot` should be stored.

    Parameters
    ----------
    output_uri : str
        A combination of S3 bucket and object directory prefix
    ipppssoot : str
        HST-style dataset name for which outputs will be stored.

    Returns
    -------
    full_s3_object_path : str
        A fully specified S3 object, including bucket, directory, and filename.

    >>> get_output_path("s3://temp/batch-2020-02-13T10:33:00", "IC0B02020")
    's3://temp/batch-2020-02-13T10:33:00/wfc3/IC0B02020'
    """
    instrument_name = get_instrument(ipppssoot)
    return output_uri + "/" + instrument_name + "/" + ipppssoot

# -------------------------------------------------------------


def upload_filepath(filepath, s3_filepath):
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
    client = boto3.client('s3')
    if s3_filepath.startswith("s3://"):
        s3_filepath = s3_filepath[5:]
    parts = s3_filepath.split("/")
    bucket, objectname = parts[0], "/".join(parts[1:])
    with open(filepath, "rb") as f:
        client.upload_fileobj(f, bucket, objectname)

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
    ignore_err_nums : list of int
        (class) Nonzero calibration error codes which should be ignored.
    stage1 : str
        (class) Program name for basic calibration and all association or unassociated files.
    stage2 : str
        (class) Program for follow-on  processing of calibrated association member files.

    ipppssoot  : str
        (instance) Name of dataset being processed
    output_uri : str
        (instance) Root output path,  e.g. s3://bucket/subdir/subdir/.../subdir

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
    instrument_name = None     # abstract class
    download_suffixes = None   # abstract class
    ignore_err_nums = []       # abstract class
    stage1 = None              # abstract class
    stage2 = None              # abstract class

    def __init__(self, ipppssoot, output_uri):
        self.ipppssoot = ipppssoot
        self.output_uri = output_uri

    # .............................................................

    def raw_files(self, files):
        """Return each name string in `files` with includes the substring '_raw'."""
        return [f for f in files if "_raw" in f]

    def assoc_files(self, files):
        """Return each name string in `files` which ends with '_asn.fits'."""
        return [f for f in files if f.endswith("_asn.fits")]

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
        dashes = (100-len(msg)-2)
        log.info(dash * dashes)
        log.info(
            dash*5,
            self.ipppssoot, msg,
            dash*(dashes-6-len(self.ipppssoot)-len(msg)-1))

    def run(self, cmd, *args):
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
        err = subprocess.call(cmd)
        if err in self.ignore_err_nums:
            log.info("Ignoring error status =", err)
        elif err:
            log.error(self.ipppssoot, "Command:", repr(cmd), "exited with error status:", err)
            sys.exit(1)     # should be 0-127,  higher err val's like 512 are truncated to 0 by shells

    # .............................................................

    def main(self):
        """Perform all processing steps for basic calibration processing:
        1. Download uncalibrated data
        2. Assign bestrefs (and potentially download reference files)
        3. Perform stage1 and stage2 CAL processing
        4. Copy outputs to S3
        5. Issues start and stop dividers
        """
        self.divider(
            "Started processing for", self.instrument_name, self.ipppssoot)

        input_files = self.dowload()

        self.assign_bestrefs(input_files)

        self.process(input_files)

        self.output_files()

        self.divider(
            "Completed processing for", self.instrument_name, self.ipppssoot)

    def dowload(self):
        """Download any data files for the `ipppssoot`,  issuing start and
        stop divider messages.

        Returns
        -------
        filepaths : sorted list
            Local file system paths of files which were downloaded for `ipppssoot`,
            some of which will be selected for calibration processing.
        """
        self.divider("Retrieving data files for:", self.download_suffixes)
        files = retrieve_observation(self.ipppssoot, suffix=self.download_suffixes)
        self.divider("Download data complete.")
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
        self.divider("Computing bestrefs and downloading references.", files)
        bestrefs_files = self.raw_files(files)
        bestrefs.assign_bestrefs(bestrefs_files, sync_references=True)
        self.divider("Bestrefs complete.")

    def process(self, files):
        """Runs each filepath in `files` through calibration processing.   Association
        files are run through both stage1 and stage2 calibration programs,
        Unassociated files are run through only the stage1 program.

        Parameters
        ----------
        files : list of str
            Filepaths of files to filter and calibrate as appropriate.

        Returns
        -------
        None
        """
        assoc = self.assoc_files(files)
        if assoc:
            self.run(self.stage1, *assoc)
            if self.stage2:
                self.run(self.stage2, *assoc)
            return
        unassoc = self.unassoc_files(files)
        if unassoc:
            self.run(self.stage1, *unassoc)

    def output_files(self):
        """Selects files from the current working directory and uploads them
        to the `output_uri`.   If `output_uri` is None or "none",  returns
        immediately without copying files.

        Returns
        -------
        None
        """
        if self.output_uri in [None, "none"]:
            return
        outputs = glob.glob("*.fits")
        outputs += glob.glob("*.tra")
        self.divider("Saving outputs:", self.output_uri, outputs)
        output_path = get_output_path(self.output_uri, self.ipppssoot)
        for filename in outputs:
            upload_filepath(filename, output_path + "/" + filename)
        self.divider("Saving outputs complete.")

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
    download_suffixes = ["ASN", "RAW", "EPC", "SPT",
                         "RAWACCUM", "RAWACCUM_A", "RAWACCUM_B",
                         "RAWACQ",
                         "RAWTAG", "RAWTAG_A", "RAWTAG_B"]
    stage1 = "calcos"
    stage2 = None
    ignore_err_nums = [
        5,    # Ignore calcos errors from RAWACQ
    ]

    def raw_files(self, files):
        """Customize to set RANSEED to 1 in raw files for consistent outputs."""
        raw = super(CosManager, self).raw_files(files)
        for f in raw:
            fits.setval(f, "RANSEED", value=1)
        return raw

    def unassoc_files(self, files):
        """Returns only the first file returned by raw_files()."""
        return super(CosManager, self).raw_files(files)[:1]   # return only first file


class StisManager(InstrumentManager):
    """Manages calibration for one"""
    instrument_name = "stis"
    download_suffixes = ["ASN", "RAW", "EPC", "TAG", "WAV"]
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
        raw = [f for f in files if f.endswith("_raw.fits")]
        wav = [f for f in files if f.endswith("_wav.fits")]
        if raw:
            self.run(self.stage1, *raw)
        else:
            self.run(self.stage1, *wav)

    def raw_files(self, files):
        """Returns only '_raw.fits', '_wav.fits', or '_tag.fits' members of `files`."""
        return [f for f in files if f.endswith(('_raw.fits','_wav.fits','_tag.fits'))]

# ............................................................................

MANAGERS = {
    "acs": AcsManager,
    "cos": CosManager,
    "stis": StisManager,
    "wfc3": Wfc3Manager,
}


def get_instrument_manager(ipppssoot, output_uri):
    """Given and `ipppssoot` and `output_uri`,  determine
    the appropriate instrument manager from the `ipppssoot`
    and construct and return it.

    Parameters
    ----------
    ipppssoot : str
        The HST dataset name to be processed.
    output_uri : str
        The base path to which outputs will be copied, nominally S3://bucket/subdir/.../subdir

    Returns
    -------
    instrument_manager : InstrumentManager subclass
        The instrument-specific InstrumentManager subclass instance appropriate for
        processing dataset name `ipppssoot`.
    """
    instrument = get_instrument(ipppssoot)
    manager = MANAGERS[instrument](ipppssoot, output_uri)
    return manager

# -----------------------------------------------------------------------------

def process(ipppssoot, output_uri):
    """Given an `ipppssoot` and `output_uri` where products should be stored,
    perform all required processing steps for the `ipppssoot` and store all
    products to `output_uri`.

    Parameters
    ----------
    ipppssoot : str
        The HST dataset name to be processed.
    output_uri : str
        The base path to which outputs will be copied, nominally S3://bucket/subdir/.../subdir

    Returns
    -------
    None
    """
    manager = get_instrument_manager(ipppssoot, output_uri)
    manager.main()

# -----------------------------------------------------------------------------


def process_ipppssoots(ipppssoots, output_uri=None):
    """Given a list of `ipppssoots` dataset names,  and an S3 `output_uri` defining
    the S3 base path at which to store outputs,  calibrate data corresponding to
    each of the ipppssoots,  including any association members.

    Parameters
    ----------
    ipppssoots : list of str
        HST dataset names to process, e.g.  [ 'J8CB010B0', ...]

    output_uri:  str
        S3 bucket and object prefix
        e.g. 's3://hstdp-batch-outputs/batch-1-2020-06-11T19-35-51'

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
        process(ipppssoot, output_uri)

# -----------------------------------------------------------------------------


def test():
    from caldp import process
    import doctest
    return doctest.testmod(process)

# -----------------------------------------------------------------------------


if __name__ == "__main__":
    if  len(sys.argv) < 3:
        print("usage:  process.py  <output_uri>   <ipppssoot's...>")
        sys.exit(1)
    output_uri = sys.argv[1]
    ipppssoots = sys.argv[2:]
    if output_uri.lower() == "none":
        output_uri = None
    process_ipppssoots(ipppssoots, output_uri)
