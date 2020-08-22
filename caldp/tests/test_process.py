"""This module defines tests for the process.py module which handles obtaining data,
assigning references, and basic calibrations.
"""
import os
import glob
import subprocess

import pytest

from caldp import process
from caldp import main

# ----------------------------------------------------------------------------------------

# For applicable tests,  the product files associated with each ipppssoot below
# must be present in the CWD after processing and be within 10% of the listed sizes.
RESULTS = {
    "j8cb010b0": """
61509 j8cb010b0.tra
11520 j8cb010b0_asn.fits
5023 j8cb010b1.tra
10535040 j8cb010b1_crj.fits
15963840 j8cb010b1_drz.fits
3145 j8cb01u2q.tra
10820160 j8cb01u2q_flt.fits
51840 j8cb01u2q_flt_hlet.fits
2257920 j8cb01u2q_raw.fits
2818 j8cb01u3q.tra
10820160 j8cb01u3q_flt.fits
51840 j8cb01u3q_flt_hlet.fits
2257920 j8cb01u3q_raw.fits
""",
    "octka6010": """
1638720 octka6010_x1d.fits
10535040 octka6010_flt.fits
4262400 octka6010_wav.fits
8429760 octka6010_raw.fits
11520 octka6010_asn.fits
""",
    "o8l7sws9q": """
12147840 o8l7sws9q_x2d.fits
10535040 o8l7sws9q_flt.fits
2257920 o8l7sws9q_raw.fits
""",
    "ldqhpbi9q": """
959 ldqhpbi9q.tra
37823040 ldqhpbi9q_counts.fits
37823040 ldqhpbi9q_flt.fits
8464320 ldqhpbi9q_rawacq.fits
316800 ldqhpbi9q_spt.fits
""",
}

#     "idgg23ztq": """
# 100  fix.me  long running???  get file definitions from test output
# """


LONG_TEST_IPPPSSOOTS = list(RESULTS.keys())
SHORT_TEST_IPPPSSOOTS = list(RESULTS.keys())[:1]

# ----------------------------------------------------------------------------------------


def parse_results(results):
    """Break up multiline output from ls -1st into a list of (name, size) tuples."""
    return [(line.split()[1], int(line.split()[0])) for line in results.splitlines() if line.strip()]


def check_results(ipppssoot):
    """For the given `ipppssoot`,  verify that all of the files listed in results
    both exist in the CWD and have sizes within 10% of the recorded values.
    """
    expected = parse_results(RESULTS[ipppssoot])
    working_files = {os.path.basename(path): os.stat(path).st_size for path in glob.glob("*")}
    for (expected_name, expected_size) in expected:
        assert expected_name in working_files
        if expected_name.startswith("s3://"):
            continue
        assert abs(working_files[expected_name] - expected_size) < 0.1 * expected_size


@pytest.mark.parametrize("ipppssoot", LONG_TEST_IPPPSSOOTS)
def test_ipppssoot_user(tmpdir, ipppssoot):
    """Run every `ipppssoot` through process.process() downloading input files from
    astroquery and writing output files to local storage.   Verify that call to process
    outputs the expected files with reasonable sizes into it's CWD.
    """
    working_dir = tmpdir.mkdir(ipppssoot)
    old_dir = working_dir.chdir()
    os.mkdir("outputs")
    try:
        main.main(ipppssoot, "astroquery:", "file:outputs")
        os.system("/bin/ls -1st *.fits *.tra && find inputs && find outputs")
        check_results(ipppssoot)
    finally:
        old_dir.chdir()


@pytest.mark.parametrize("ipppssoot", SHORT_TEST_IPPPSSOOTS)
def test_ipppssoot_pipeline(tmpdir, ipppssoot):
    """Run every `ipppssoot` through process.process() downloading input files from
    astroquery and writing output files to local storage.   Verify that call to process
    outputs the expected files with reasonable sizes into it's CWD.
    """
    working_dir = tmpdir.mkdir(ipppssoot)
    old_dir = working_dir.chdir()
    os.mkdir("outputs")
    os.mkdir("inputs")
    try:
        process.download_inputs(ipppssoot, "file:inputs", "file:outputs")  # get inputs seperately
        main.main(ipppssoot, "file:inputs", "file:outputs")
        os.system("/bin/ls -1st *.fits *.tra && find inputs && find outputs")
        check_results(ipppssoot)
    finally:
        old_dir.chdir()


@pytest.mark.parametrize("ipppssoot", SHORT_TEST_IPPPSSOOTS)
def test_ipppssoot_aws(tmpdir, ipppssoot):
    """Run every `ipppssoot` through process.process() downloading input files from
    astroquery and writing output files to local storage.   Verify that call to process
    outputs the expected files with reasonable sizes into it's CWD.
    """
    working_dir = tmpdir.mkdir(ipppssoot)
    s3_path = f"s3://calcloud-hst-test-outputs/test-batch"
    os.system("aws s3 rm --recursive {s3_path}")
    old_dir = working_dir.chdir()
    try:
        main.main(ipppssoot, "astroquery:", s3_path)
        os.system("/bin/ls -1st *.fits *.tra")
        os.system("aws s3 ls --recursive " + s3_path)
        check_results(ipppssoot)
    finally:
        old_dir.chdir()
