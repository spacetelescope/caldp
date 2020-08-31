"""This module defines tests for the process.py module which handles obtaining data,
assigning references, and basic calibrations.
"""
import os
import sys
import subprocess

import pytest

from caldp import process
from caldp import main

# ----------------------------------------------------------------------------------------

# For applicable tests,  the product files associated with each ipppssoot below
# must be present in the CWD after processing and be within 10% of the listed sizes.
RESULTS = [
    (
        "j8cb010b0",
        """
18457 inputs/j8cb010b1_crj.jpg
1036 inputs/j8cb010b1_crj_thumb.jpg
31941 inputs/j8cb010b1_drz.jpg
1920 inputs/j8cb010b1_drz_thumb.jpg
21728 inputs/j8cb01u3q_raw.jpg
1156 inputs/j8cb01u3q_raw_thumb.jpg
18712 inputs/j8cb01u2q_flt.jpg
1084 inputs/j8cb01u2q_flt_thumb.jpg
21587 inputs/j8cb01u2q_raw.jpg
1159 inputs/j8cb01u2q_raw_thumb.jpg
18748 inputs/j8cb01u3q_flt.jpg
1083 inputs/j8cb01u3q_flt_thumb.jpg
16493 inputs/j8cb010b0.tra
51840 inputs/j8cb01u3q_flt_hlet.fits
51840 inputs/j8cb01u2q_flt_hlet.fits
15963840 inputs/j8cb010b1_drz.fits
10820160 inputs/j8cb01u3q_flt.fits
10820160 inputs/j8cb01u2q_flt.fits
2818 inputs/j8cb01u3q.tra
3547 inputs/j8cb01u2q.tra
5446 inputs/j8cb010b1.tra
10535040 inputs/j8cb010b1_crj.fits
11520 inputs/j8cb010b0_asn.fits
2257920 inputs/j8cb01u3q_raw.fits
2257920 inputs/j8cb01u2q_raw.fits
18457 outputs/acs/j8cb010b0/previews/j8cb010b1_crj.jpg
1036 outputs/acs/j8cb010b0/previews/j8cb010b1_crj_thumb.jpg
31941 outputs/acs/j8cb010b0/previews/j8cb010b1_drz.jpg
1920 outputs/acs/j8cb010b0/previews/j8cb010b1_drz_thumb.jpg
21728 outputs/acs/j8cb010b0/previews/j8cb01u3q_raw.jpg
1156 outputs/acs/j8cb010b0/previews/j8cb01u3q_raw_thumb.jpg
18712 outputs/acs/j8cb010b0/previews/j8cb01u2q_flt.jpg
1084 outputs/acs/j8cb010b0/previews/j8cb01u2q_flt_thumb.jpg
21587 outputs/acs/j8cb010b0/previews/j8cb01u2q_raw.jpg
1159 outputs/acs/j8cb010b0/previews/j8cb01u2q_raw_thumb.jpg
18748 outputs/acs/j8cb010b0/previews/j8cb01u3q_flt.jpg
1083 outputs/acs/j8cb010b0/previews/j8cb01u3q_flt_thumb.jpg
2257920 outputs/acs/j8cb010b0/j8cb01u3q_raw.fits
51840 outputs/acs/j8cb010b0/j8cb01u3q_flt_hlet.fits
10820160 outputs/acs/j8cb010b0/j8cb01u3q_flt.fits
2818 outputs/acs/j8cb010b0/j8cb01u3q.tra
2257920 outputs/acs/j8cb010b0/j8cb01u2q_raw.fits
51840 outputs/acs/j8cb010b0/j8cb01u2q_flt_hlet.fits
10820160 outputs/acs/j8cb010b0/j8cb01u2q_flt.fits
3547 outputs/acs/j8cb010b0/j8cb01u2q.tra
15963840 outputs/acs/j8cb010b0/j8cb010b1_drz.fits
10535040 outputs/acs/j8cb010b0/j8cb010b1_crj.fits
5446 outputs/acs/j8cb010b0/j8cb010b1.tra
11520 outputs/acs/j8cb010b0/j8cb010b0_asn.fits
16493 outputs/acs/j8cb010b0/j8cb010b0.tra
""",
    ),
    (
        "octka6010",
        """
8141 outputs/stis/octka6010/previews/octka6010_x1d_thumb.png
271687 outputs/stis/octka6010/previews/octka6010_x1d.png
1638720 outputs/stis/octka6010/octka6010_x1d.fits
4262400 outputs/stis/octka6010/octka6010_wav.fits
8429760 outputs/stis/octka6010/octka6010_raw.fits
10535040 outputs/stis/octka6010/octka6010_flt.fits
11520 outputs/stis/octka6010/octka6010_asn.fits
""",
    ),
    (
        "o8l7sws9q",
        """
10535040 outputs/stis/o8l7sws9q/o8l7sws9q_flt.fits
2257920 outputs/stis/o8l7sws9q/o8l7sws9q_raw.fits
12147840 outputs/stis/o8l7sws9q/o8l7sws9q_x2d.fits
""",
    ),
    (
        "ldqhpbi9q",
        """
37823040 outputs/cos/ldqhpbi9q/ldqhpbi9q_counts.fits
959 outputs/cos/ldqhpbi9q/ldqhpbi9q.tra
37823040 outputs/cos/ldqhpbi9q/ldqhpbi9q_flt.fits
316800 outputs/cos/ldqhpbi9q/ldqhpbi9q_spt.fits
8464320 outputs/cos/ldqhpbi9q/ldqhpbi9q_rawacq.fits
""",
    ),
]

#     "idgg23ztq": """
# 100  fix.me  long running???  get file definitions from test output
# """


SHORT_TEST_IPPPSSOOTS = [result[0] for result in RESULTS][:1]
LONG_TEST_IPPPSSOOTS = [result[0] for result in RESULTS][1:]

LONG_TEST_IPPPSSOOTS += SHORT_TEST_IPPPSSOOTS  # Include all for creating test cases.


# Leave S3 config undefined to skip S3 tests
CALDP_S3_TEST_OUTPUTS = os.environ.get("CALDP_S3_TEST_OUTPUTS")  # s3://calcloud-hst-test-outputs/test-batch
CALDP_S3_TEST_INPUTS = os.environ.get("CALDP_S3_TEST_INPUTS")

# Output sizes must be within +- this fraction of truth value
CALDP_TEST_FILE_SIZE_THRESHOLD = float(os.environ.get("CALDP_TEST_FILE_SIZE_THRESHOLD", 0.4))

# ----------------------------------------------------------------------------------------


@pytest.mark.parametrize("output_uri", ["file:outputs", CALDP_S3_TEST_OUTPUTS])
@pytest.mark.parametrize("input_uri", ["file:inputs", "astroquery:", CALDP_S3_TEST_INPUTS])
@pytest.mark.parametrize("ipppssoot", SHORT_TEST_IPPPSSOOTS)
def test_io_modes(tmpdir, ipppssoot, input_uri, output_uri):
    coretst(tmpdir, ipppssoot, input_uri, output_uri)


@pytest.mark.parametrize("output_uri", ["file:outputs"])
@pytest.mark.parametrize("input_uri", ["astroquery:"])
@pytest.mark.parametrize("ipppssoot", LONG_TEST_IPPPSSOOTS)
def test_instruments(tmpdir, ipppssoot, input_uri, output_uri):
    coretst(tmpdir, ipppssoot, input_uri, output_uri)


def coretst(temp_dir, ipppssoot, input_uri, output_uri):
    """Run every `ipppssoot` through process.process() downloading input files from
    astroquery and writing output files to local storage.   Verify that call to process
    outputs the expected files with reasonable sizes into it's CWD.
    """
    print("Test case", ipppssoot, input_uri, output_uri)
    if not input_uri or not output_uri:
        print("Skipping case", ipppssoot, input_uri, output_uri)
        return
    working_dir = temp_dir.mkdir(ipppssoot)
    old_dir = working_dir.chdir()
    try:
        setup_io(ipppssoot, input_uri, output_uri)
        main.main(ipppssoot, input_uri, output_uri)
        list_files(ipppssoot, input_uri, output_uri)
        check_results(ipppssoot, input_uri, output_uri)
    finally:
        old_dir.chdir()


# ----------------------------------------------------------------------------------------


def parse_results(results):
    """Break up multiline output from ls into a list of (name, size) tuples."""
    return [(line.split()[1], int(line.split()[0])) for line in results.splitlines() if line.strip()]


def setup_io(ipppssoot, input_uri, output_uri):
    os.mkdir("outputs")
    if input_uri.startswith("file:"):
        input_dir = input_uri.split(":")[-1]
        os.mkdir(input_dir)
        process.download_inputs(ipppssoot, input_uri, output_uri)  # get inputs seperately


def list_files(ipppssoot, input_uri, output_uri):
    """Routinely log input, output, and CWD files to aid setting up expected results."""
    outputs = list_fs("inputs")
    outputs += list_fs("outputs")
    if CALDP_S3_TEST_INPUTS and input_uri.lower().startswith("s3://"):
        outputs += list_s3(CALDP_S3_TEST_INPUTS)
    if CALDP_S3_TEST_OUTPUTS and output_uri.lower().startswith("s3://"):
        outputs += list_s3(CALDP_S3_TEST_OUTPUTS)
    sys.stdout.flush()
    sys.stderr.flush()
    print(outputs)
    sys.stdout.flush()
    sys.stderr.flush()
    return parse_results(outputs)


def list_fs(path):
    """List local files at `path` for defining truth data and actual files."""
    return chain(f"/usr/bin/find {path} -type f", "xargs ls -lt", "awk -e {print($5,$9);}")


def list_s3(path):
    """List S3 files at `path` for defining truth data and actual files."""
    output = chain(f"aws s3 ls --recursive {path}", "awk -e {print($3,$4);}")
    return output.replace(" ", " outputs/")


def chain(*args, encoding="utf-8", print_output=False):
    """Every arg should be a subprocess command string which will be run and piped to
    any subsequent args in a linear process chain.  Each arg will be split into command
    words based on whitespace so whitespace embedded within words is not possible.

    Process error handling is undefined and may not raise exceptions.

    Returns stdout from the chain.
    """
    pipes = []
    for cmd in args:
        words = cmd.split()
        if pipes:
            p = subprocess.Popen(words, stdin=pipes[-1].stdout, stdout=subprocess.PIPE)
            pipes[-1].stdout.close()
        else:
            p = subprocess.Popen(words, stdout=subprocess.PIPE)
        pipes.append(p)
    output = p.communicate()[0]
    output = output.decode(encoding) if encoding else output
    if print_output:
        print(output, end="")
    pipes[-1].stdout.close()
    return output


def check_results(ipppssoot, input_uri, output_uri):
    """For the given `ipppssoot`,  verify that all of the files listed in results
    both exist in the CWD and have sizes within 10% of the recorded values.
    """
    results = dict(RESULTS)
    expected = parse_results(results[ipppssoot])
    actual_files = dict(list_files(ipppssoot, input_uri, output_uri))
    for (expected_name, expected_size) in expected:
        if expected_name.startswith("inputs/") and not input_uri.startswith("file:"):
            continue
        assert expected_name in actual_files
        if expected_name.startswith("s3://"):
            continue
        assert (
            abs(actual_files[expected_name] - expected_size) < CALDP_TEST_FILE_SIZE_THRESHOLD * expected_size
        ), "bad size for " + repr(expected_name)
