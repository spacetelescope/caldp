# This module creates preview images from reprocessing output data.

import argparse
import os
import re
import subprocess
import logging
import glob
import shutil
import boto3
import statistics

from astropy.io import fits

from caldp import log
from caldp import process
from caldp import file_ops
from caldp import messages
from caldp import sysexit

# -------------------------------------------------------------------------------------------------------

LOGGER = logging.getLogger(__name__)
HAP_PREFIX = "hst"
HAP_SEP = "_"

# -------------------------------------------------------------------------------------------------------


def get_suffix(suffix_param):
    """Returns the suffixes of files to generate previews"""
    if suffix_param == "stis":
        req_sfx = ["x1d", "sx1"]
    elif suffix_param == "cos":
        req_sfx = ["x1d", "x1dsum"] + ["x1dsum" + str(x) for x in range(1, 5)]
    elif suffix_param == "acs":
        req_sfx = ["crj", "drc", "drz", "raw", "flc", "flt"]
    elif suffix_param == "wfc3":
        req_sfx = [
            "drc",
            "drz",
            "flc",
            "flt",
            "ima",
            "raw",
        ]
    elif suffix_param in ["svm", "mvm"]:
        req_sfx = ["drc", "drz"]
    else:
        req_sfx = ""
    return req_sfx


# -------------------------------------------------------------------------------------------------------


class PreviewManager:
    """Abstract preview manager baseclass which is customized based on dataset type.
    Further customizations are applied as by overriding baseclass methods.

    Attributes
    ----------
    dataset  : str
        (instance) Name of dataset being processed
    input_uri_prefix : str
        (instance) root input path
    output_uri_prefix : str
        (instance) Root output path,  e.g. s3://bucket/subdir/subdir/.../subdir

    search_input_pattern : str
        pattern to use to search for input files e.g. f"{self.dataset.lower()[0:5]}*.fits"
    suffix_param : str
        parameter used to retrieve file suffixes for generating previews from get_suffix()
    output_formats : list of tuples
        preview suffixes and sizes

    Methods
    -------
    __init__(dataset, input_uri_prefix, output_uri_prefix)
    generate_image_preview(input_path, output_path, size, autoscale, more_options)
        Runs fitscut command with given args
    generate_image_previews(input_path, filename_base)
        Calls generate_image_preview() for each output format
    generate_spectral_previews(input_path)
        Runs make_hst_spec_previews
    get_inputs()
        Searches for potential preview inputs using search_input_pattern
    get_preview_inputs(input_paths)
        Filter input files using req_sfx from get_suffix()
    get_previews()
        Returns a list of previews files generated
    create_previews(preview_inputs)
        Abstract class called by main that generates previews,
        customized based on dataset type (HAP or ipppssoot)
    upload_previews(previews, output_path)
        Upload previews to S3
    copy_previews(previews, output_path)
        Copy previews to output_path
    main()
        Generates previews based on input and output directories
        according to specified args



    """

    def __init__(self, dataset, input_uri_prefix, output_uri_prefix):
        self.dataset = dataset
        self.input_uri_prefix = input_uri_prefix
        self.output_uri_prefix = output_uri_prefix
        self.input_dir = file_ops.get_input_path(input_uri_prefix, dataset)
        self.search_input_pattern = None  # Abstract class
        self.suffix_param = None  # Abstract class
        self.output_formats = [("_thumb", 128), ("", -1)]

    def generate_image_preview(self, input_path, output_path, size, autoscale=99.5, more_options=None):
        cmd = [
            "fitscut",
            "--all",
            "--jpg",
            f"--autoscale={autoscale}",
            "--asinh-scale",
            f"--output-size={size}",
            "--badpix",
        ]

        if more_options:
            cmd.append(more_options)

        if input_path:
            cmd.append(input_path)

        cmd = " ".join(cmd)
        process = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, stderr = process.communicate()

        if process.returncode > 0:
            LOGGER.error("fitscut failed for %s with status %s: %s", input_path, process.returncode, stderr)
            raise RuntimeError()

        with open(output_path, "wb") as f:
            f.write(stdout)

    def generate_image_previews(self, input_path, filename_base):
        for suffix, size in self.output_formats:
            output_path = os.path.join(self.input_dir, f"{filename_base}{suffix}.jpg")
            try:
                self.generate_image_preview(input_path, output_path, size)
            except Exception:
                log.info("Preview file (imaging) not generated for", input_path, "with size", size)

    def generate_spectral_previews(self, input_path):
        cmd = ["make_hst_spec_previews", "-v", "-t", "png", "fits", "-o", self.input_dir, input_path]
        err = subprocess.call(cmd)
        if err:
            LOGGER.exception(f"Preview file not generated for {input_path}")
            return []

    def get_inputs(self):
        search_fits = f"{self.input_dir}/{self.search_input_pattern}"
        inputs = glob.glob(search_fits)
        return list(sorted(inputs))

    def get_preview_inputs(self, input_paths):
        req_sfx = get_suffix(self.suffix_param)
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

    def get_previews(self):
        png_search = f"{self.input_dir}/*.png"
        jpg_search = f"{self.input_dir}/*.jpg"
        prev_search = f"{self.input_dir}/*_prev.fits"
        preview_files = glob.glob(png_search)
        preview_files.extend(glob.glob(jpg_search))
        preview_files.extend(glob.glob(prev_search))
        return list(sorted(preview_files))

    def create_previews(self, preview_inputs):
        """Abstract class called by main that generates previews based on s3 downloads
        Returns a list of file paths to previews
        """
        pass

    def upload_previews(self, previews, output_path):
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

    def copy_previews(self, previews, output_path):
        for filepath in previews:
            preview_file = os.path.join(output_path, os.path.basename(filepath))
            shutil.copy(filepath, preview_file)
            log.info(f"\t{preview_file}")
        os.listdir(output_path)

    def main(self):
        """Generates previews based on input and output directories
        according to specified args
        """
        output_path = messages.get_local_outpath(self.output_uri_prefix, self.dataset)
        msg = messages.Messages(self.output_uri_prefix, output_path, self.dataset)
        msg.preview_message()  # processing
        log.init_log("preview.txt")
        # get input paths
        input_paths = self.get_inputs()
        preview_inputs = self.get_preview_inputs(input_paths)
        # create previews
        previews = self.create_previews(preview_inputs)
        # upload/copy previews
        log.info("Saving previews...")
        if self.output_uri_prefix.startswith("s3"):
            preview_output = process.get_output_path("file:outputs", self.dataset) + "/previews"
            os.makedirs(preview_output, exist_ok=True)
            self.copy_previews(previews, preview_output)
            log.info("Preparing files for s3 upload...")
            file_ops.tar_outputs(self.dataset, self.input_uri_prefix, self.output_uri_prefix)
        elif self.output_uri_prefix.startswith("file"):
            preview_output = process.get_output_path(self.output_uri_prefix, self.dataset) + "/previews"
            os.makedirs(preview_output, exist_ok=True)
            self.copy_previews(previews, preview_output)
        log.close_log()


class IpstPreviewManager(PreviewManager):
    def __init__(self, dataset, input_uri_prefix, output_uri_prefix):
        super().__init__(dataset, input_uri_prefix, output_uri_prefix)
        self.search_input_pattern = f"{self.dataset.lower()[0:5]}*.fits"
        self.suffix_param = process.get_instrument(dataset)

    def create_previews(self, preview_inputs):
        """Generates previews based on s3 downloads
        Returns a list of file paths to previews
        """
        log.info("Processing", len(preview_inputs), "FITS file(s) from ", self.input_dir)
        # Generate previews to local preview folder inside ipppssoot folder
        for input_path in preview_inputs:
            log.info("Generating previews for", input_path)
            filename_base = os.path.basename(input_path).split(".")[0]
            self.generate_previews(input_path, filename_base)
        # list of full paths to preview files
        previews = self.get_previews()
        log.info("Generated", len(previews), "preview files")
        return previews

    def generate_previews(self, input_path, filename_base):
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
            return self.generate_spectral_previews(input_path)
        elif naxis >= 2 and ext == "IMAGE" and instr_char not in ["l", "o"]:
            print("Generating image previews...")
            return self.generate_image_previews(input_path, filename_base)
        else:
            log.warning("Unable to determine FITS file type")
            return []


class HapPreviewManager(PreviewManager):
    def __init__(self, dataset, input_uri_prefix, output_uri_prefix):
        super().__init__(dataset, input_uri_prefix, output_uri_prefix)
        self.dataset_type = process.get_dataset_type(dataset)
        if self.dataset_type == "svm":
            self.ipppss = process.get_svm_obs_set(self.dataset)
            self.search_input_pattern = f"hst_*{self.ipppss}*.fits"
        elif self.dataset_type == "mvm":
            self.search_input_pattern = f"hst_{self.dataset.lower()}*.fits"
            self.output_formats = [("_thumb", 512), ("", -1)]
        self.suffix_param = self.dataset_type
        self.filters_file_path = find_file("ACS_WFC3_filters.txt", os.path.expanduser("~"))
        # if os.path.exists(os.path.join(os.path.expanduser("~"), "caldp/ACS_WFC3_filters.txt")):
        #     self.filters_file_path = os.path.join(os.path.expanduser("~"), "caldp/ACS_WFC3_filters.txt")
        # else:
        #     self.filters_file_path = find_file("ACS_WFC3_filters.txt", os.path.expanduser("~"))
        
        self.acs_wfc3_filters = {}

    def determine_data_type(self, fitsfile):
        """Determine the type of data in a FITS file; returns 'IMAGE' or None."""
        hdu = fits.open(fitsfile)
        naxis = hdu[1].header["NAXIS"]
        ext = hdu[1].header["XTENSION"]
        hdu.close()

        if naxis >= 2 and ext == "IMAGE":
            data_type = "IMAGE"
        else:
            data_type = None
        return data_type

    def get_previewable_inputs(self, input_paths):
        """Identify FITS files for which previews can be generated."""
        preview_inputs = []
        for input_path in input_paths:
            data_type = self.determine_data_type(input_path)
            if data_type:
                preview_inputs.append(input_path)
        return preview_inputs

    def make_mosaic_name(self):
        if self.dataset_type == "svm":
            return self.ipppss
        elif self.dataset_type == "mvm":
            return self.dataset

    def color_pattern(self):
        """Return pattern to select files for the given mosaic type."""
        # mosaic_name only applies to SVM pattern therefore defaults to None
        if self.dataset_type == "svm":
            regex = (HAP_SEP).join(
                [
                    HAP_PREFIX,
                    r"(\d{4,5})",  # 1 propid
                    r"(\d{2})",  # 2 visit
                    r"([^\s]+)",  # 3 instrument
                    r"([^\s]+)",  # 4 det
                    r"([a-z]\d{3}[a-z])",  # 5 filter
                    self.mosaic_name,  # mosaic_name (already a var, no need to capture)
                    r"([a-z]{3}).fits",  # 6 suffix.fits
                ]
            )
            groups_of_interest = dict(propid=1, visit=2, inst=3, det=4, filt=5, sufx=6)
        elif self.dataset_type == "mvm":
            # Expected filenames based on documentation:
            # drizzle hst_skycell-p<PPPP>x<XX>y<YY>_<instr>_<detector>_<filter>_<label>_dr[cz].fits
            # preview hst_skycell-p<PPPP>x<XX>y<YY>_<instr>_<detector>_<filter>_<label>_dr[cz][_thumb].jpg
            # weightd hst_skycell-p<PPPP>x<XX>y<YY>_<instr>_<detector>_<filter>_<label>_wht.jpg
            # trailer hst_skycell-p<PPPP>x<XX>y<YY>_<instr>_<detector>_<filter>_<label>_trl.txt
            # Additional details on layers, cellnames, and labels at:
            # https://innerspace.stsci.edu/display/hstdms/HLA+Multi-visit+Filenames
            #
            # Reality not truly matching documentation, expected to improve after HLA-440 is worked
            # The files are not consistently named and double filters cause issues
            # so cannot do: #r'hst_(skycell-p\d{4}x\d{2}y\d{2})_([^\s]+)_([^\s])_([a-z]\d{3}[a-z])_([^\s])_([a-z]{3}).fits'
            # NOTE: current set of example mvm files as provided had to be renamed for this to match
            # all the files
            regex = (HAP_SEP).join(
                [
                    HAP_PREFIX,
                    r"(skycell-p\d{4}x\d{2}y\d{2})",  # 1 skycell
                    r"([a-z]{3}[0-9]{0,1})",  # 2 instrument
                    r"([a-z]{3,4})",  # 3 detector
                    r"([a-z]\d{3}[a-z])",  # 4 afilter
                    r"([^\s]+)",  # 5 label
                    r"([a-z]{3})",  # 6 suffix
                ]
            )
            groups_of_interest = dict(skycell=1, inst=2, det=3, filt=4, label=5, sufx=6)
        return regex, groups_of_interest

    def load_acs_wfc3_filters_dict(self):
        """Load dictionary of ACS & WFC3 filters and wavelengths from text file."""

        # open filter/wavelength text table to map the filter names to the wavelength
        with open(self.filters_file_path) as filter_wavelength_file:
            for line in filter_wavelength_file:
                # regex to split every line that doesn't start with '#' by the whitespace
                match_obj = re.match(r"^(?<!#)(\w+)\W+(\w+)\W+(\w+)", line)
                if match_obj:
                    # Add wavelength to dictionary, with key: instrument_filter (e.g., ACS_F250W)
                    inst_filter_key = f"{match_obj.group(1)}_{match_obj.group(2)}"
                    wavelength_value = int(match_obj.group(3))
                    self.acs_wfc3_filters[inst_filter_key] = wavelength_value

    def make_color_preview(self, preview_inputs):
        cwd = os.getcwd()
        os.chdir(self.input_dir)

        wavelength_filename_dict = {}
        self.load_acs_wfc3_filters_dict()
        self.mosaic_name = self.make_mosaic_name()
        inst_filter_regex, gpnums = self.color_pattern()

        preview_inputs = [p.split("/")[-1] for p in preview_inputs]

        for preview_input in preview_inputs:
            matched_filename = re.match(inst_filter_regex, preview_input)
            if matched_filename:
                # Common fields between SVM and MVM
                inst = matched_filename.group(gpnums["inst"])
                filt = matched_filename.group(gpnums["filt"])
                sufx = matched_filename.group(gpnums["sufx"])
                det = matched_filename.group(gpnums["det"])
                inst_filter = f"{inst}_{filt}"

                wavelength = self.acs_wfc3_filters[inst_filter.upper()]
                wavelength_filename_dict.update({wavelength: preview_input})

        if len(wavelength_filename_dict) < 2:
            log.info(
                self.dataset,
                "There are not enough FITS files in this set " "(requires at least 2) to create a color preview.",
            )
            return

        # call fitscut to create color preview
        # red_file = highest wavelength, blue_file = lowest wavelength
        red_file = wavelength_filename_dict[max(wavelength_filename_dict.keys())]
        blue_file = wavelength_filename_dict[min(wavelength_filename_dict.keys())]
        # if there are only 2 files use red & blue:
        color_options = f"--red={red_file} --blue={blue_file}"

        # if there are more than 2 files, green (median) will be used as well
        if len(wavelength_filename_dict) > 2:
            # green_file = median wavelength; because 'median' will take the average of the middle
            # two values if there is an even number of values, use median_low (which takes the
            # lower of the middle two values) to ensure that the resulting value is a valid key
            # (median_high is also possible)
            green_file = wavelength_filename_dict[statistics.median_low(wavelength_filename_dict.keys())]
            color_options = f"{color_options} --green={green_file}"

        # create color preview output name from the RED_FILE (any color works) filename:
        matched_filename = re.match(inst_filter_regex, red_file)
        if self.dataset_type == "svm":
            # SVM-specific output filename parts (in addition to mosaic_name)
            propid = matched_filename.group(gpnums["propid"])
            visit = matched_filename.group(gpnums["visit"])
            file_id = HAP_SEP.join([propid, visit, inst, det])
            # HSTSDP-655 switched to using "color" instead of other info
            color_output_filename = HAP_SEP.join([HAP_PREFIX, file_id, "total", self.mosaic_name, sufx, "color"])
        elif self.dataset_type == "mvm":
            # MVM-specific output filename parts
            skycell = matched_filename.group(gpnums["skycell"])
            label = matched_filename.group(gpnums["label"])
            # hst_skycell-p<PPPP>x<XX>y<YY>_<instr>_<detector>_<filter>_<label>_dr[cz][_thumb].jpg
            color_output_filename = HAP_SEP.join([HAP_PREFIX, skycell, inst, det, "total", label, sufx, "color"])

        for suffix, size in self.output_formats:
            outputfile = f"{color_output_filename}{suffix}.jpg"
            output_path = os.path.join(self.input_dir, outputfile)
            # Create color preview using fitscut
            log.info(
                self.dataset, f"Generating Color Image Preview using the following color " f"inputs: {color_options}"
            )
            try:
                self.generate_image_preview(None, output_path, size, more_options=color_options)
            except (RuntimeError, IOError):
                log.info(self.dataset, f"Color preview file not generated: {outputfile}")

        os.chdir(cwd)

    def create_previews(self, input_paths):
        """Generates previews based on s3 downloads
        Returns a list of file paths to previews
        """
        preview_inputs = self.get_previewable_inputs(input_paths)
        self.make_color_preview(preview_inputs)
        for input_path in preview_inputs:
            filename_base = os.path.basename(input_path).split(".")[0]
            self.generate_image_previews(input_path, filename_base)

        previews = self.get_previews()
        log.info("Generated", len(previews), "preview files")
        return previews


PREVIEW_MANAGERS = {"ipst": IpstPreviewManager, "svm": HapPreviewManager, "mvm": HapPreviewManager}

def find_file(name, path):
    for root, dirs, files in os.walk(path):
        if name in files:
            return os.path.join(root, name)

def parse_args():
    parser = argparse.ArgumentParser(description="Create image and spectral previews")
    parser.add_argument("input_uri_prefix", help="s3 or local directory containing FITS images")
    parser.add_argument("output_uri_prefix", help="S3 URI prefix for writing previews")
    parser.add_argument("dataset", help="Dataset name")
    return parser.parse_args()


def main(dataset, input_uri_prefix, output_uri_prefix):
    dataset_type = process.get_dataset_type(dataset)
    manager = PREVIEW_MANAGERS[dataset_type]
    pm = manager(dataset, input_uri_prefix, output_uri_prefix)
    pm.main()


def cmdline():
    args = parse_args()
    if args.output_uri_prefix.lower().startswith("none"):
        if args.input_uri_prefix.startswith("file"):
            output_uri_prefix = args.input_uri_prefix
        else:
            output_uri_prefix = os.path.join(os.getcwd(), args.dataset)
    else:
        output_uri_prefix = args.output_uri_prefix

    main(args.dataset, args.input_uri_prefix, output_uri_prefix)


if __name__ == "__main__":
    with sysexit.exit_receiver():
        cmdline()
