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
        "obes03010",
        """
8486 obes03010_sx1_thumb.png
121908 obes03010_sx1.png
80640 obes03010_sx1.fits
14477760 obes03010_sx2.fits
21052800 obes03010_flt.fits
10537920 obes03010_crj.fits
2260800 obes03010_wav.fits
4501440 obes03010_raw.fits
11520 obes03010_asn.fits
121908 outputs/stis/obes03010/previews/obes03010_sx1.png
8486 outputs/stis/obes03010/previews/obes03010_sx1_thumb.png
4501440 outputs/stis/obes03010/obes03010_raw.fits
80640 outputs/stis/obes03010/obes03010_sx1.fits
2260800 outputs/stis/obes03010/obes03010_wav.fits
10537920 outputs/stis/obes03010/obes03010_crj.fits
14477760 outputs/stis/obes03010/obes03010_sx2.fits
11520 outputs/stis/obes03010/obes03010_asn.fits
21052800 outputs/stis/obes03010/obes03010_flt.fits
        """,
    ),
    (
        "la8q99030",
        """
7814 la8q99ixq_x1d_thumb.png
115200 la8q99ixq_x1d_prev.fits
234510 la8q99ixq_x1d.png
7760 la8q99030_x1dsum_thumb.png
115200 la8q99030_x1dsum_prev.fits
232974 la8q99030_x1dsum.png
7760 la8q99030_x1dsum3_thumb.png
115200 la8q99030_x1dsum3_prev.fits
233301 la8q99030_x1dsum3.png
218880 la8q99030_jnk.fits
11520 la8q99030_asn.fits
195840 la8q99030_x1dsum3.fits
195840 la8q99030_x1dsum.fits
13092480 la8q99ixq_counts.fits
13092480 la8q99ixq_flt.fits
12441600 la8q99ixq_corrtag.fits
368640 la8q99ixq_x1d.fits
4869 la8q99ixq.tra
806400 la8q99ixq_lampflash.fits
2560320 la8q99ixq_rawtag.fits
218880 la8q99ixq_spt.fits
234510 outputs/cos/la8q99030/previews/la8q99ixq_x1d.png
115200 outputs/cos/la8q99030/previews/la8q99ixq_x1d_prev.fits
7814 outputs/cos/la8q99030/previews/la8q99ixq_x1d_thumb.png
232974 outputs/cos/la8q99030/previews/la8q99030_x1dsum.png
115200 outputs/cos/la8q99030/previews/la8q99030_x1dsum_prev.fits
7760 outputs/cos/la8q99030/previews/la8q99030_x1dsum_thumb.png
233301 outputs/cos/la8q99030/previews/la8q99030_x1dsum3.png
115200 outputs/cos/la8q99030/previews/la8q99030_x1dsum3_prev.fits
7760 outputs/cos/la8q99030/previews/la8q99030_x1dsum3_thumb.png
2560320 outputs/cos/la8q99030/la8q99ixq_rawtag.fits
218880 outputs/cos/la8q99030/la8q99ixq_spt.fits
368640 outputs/cos/la8q99030/la8q99ixq_x1d.fits
13092480 outputs/cos/la8q99030/la8q99ixq_flt.fits
806400 outputs/cos/la8q99030/la8q99ixq_lampflash.fits
13092480 outputs/cos/la8q99030/la8q99ixq_counts.fits
12441600 outputs/cos/la8q99030/la8q99ixq_corrtag.fits
195840 outputs/cos/la8q99030/la8q99030_x1dsum3.fits
4869 outputs/cos/la8q99030/la8q99ixq.tra
11520 outputs/cos/la8q99030/la8q99030_asn.fits
218880 outputs/cos/la8q99030/la8q99030_jnk.fits
195840 outputs/cos/la8q99030/la8q99030_x1dsum.fits
        """,
    ),
    (
        "ib8t01010",
        """
48819 ib8t01htq_ima.jpg
1877 ib8t01htq_ima_thumb.jpg
205588 ib8t01hwq_raw.jpg
1997 ib8t01hwq_raw_thumb.jpg
27459 ib8t01010_drz.jpg
1841 ib8t01010_drz_thumb.jpg
38031 ib8t01htq_flt.jpg
1615 ib8t01htq_flt_thumb.jpg
37404 ib8t01hwq_flt.jpg
1551 ib8t01hwq_flt_thumb.jpg
206484 ib8t01htq_raw.jpg
2058 ib8t01htq_raw_thumb.jpg
46513 ib8t01hwq_ima.jpg
1859 ib8t01hwq_ima_thumb.jpg
16110 ib8t01010.tra
8640 ib8t01hwq_flt_hlet.fits
8640 ib8t01htq_flt_hlet.fits
12654720 ib8t01010_drz.fits
16623360 ib8t01hwq_flt.fits
16623360 ib8t01htq_flt.fits
11520 ib8t01010_asn.fits
4192 ib8t01hwq.tra
157746240 ib8t01hwq_ima.fits
4733 ib8t01htq.tra
157746240 ib8t01htq_ima.fits
31901760 ib8t01hwq_raw.fits
31901760 ib8t01htq_raw.fits
48819 outputs/wfc3/ib8t01010/previews/ib8t01htq_ima.jpg
1877 outputs/wfc3/ib8t01010/previews/ib8t01htq_ima_thumb.jpg
205588 outputs/wfc3/ib8t01010/previews/ib8t01hwq_raw.jpg
1997 outputs/wfc3/ib8t01010/previews/ib8t01hwq_raw_thumb.jpg
27459 outputs/wfc3/ib8t01010/previews/ib8t01010_drz.jpg
1841 outputs/wfc3/ib8t01010/previews/ib8t01010_drz_thumb.jpg
38031 outputs/wfc3/ib8t01010/previews/ib8t01htq_flt.jpg
1615 outputs/wfc3/ib8t01010/previews/ib8t01htq_flt_thumb.jpg
37404 outputs/wfc3/ib8t01010/previews/ib8t01hwq_flt.jpg
1551 outputs/wfc3/ib8t01010/previews/ib8t01hwq_flt_thumb.jpg
206484 outputs/wfc3/ib8t01010/previews/ib8t01htq_raw.jpg
2058 outputs/wfc3/ib8t01010/previews/ib8t01htq_raw_thumb.jpg
46513 outputs/wfc3/ib8t01010/previews/ib8t01hwq_ima.jpg
1859 outputs/wfc3/ib8t01010/previews/ib8t01hwq_ima_thumb.jpg
31901760 outputs/wfc3/ib8t01010/ib8t01hwq_raw.fits
157746240 outputs/wfc3/ib8t01010/ib8t01hwq_ima.fits
8640 outputs/wfc3/ib8t01010/ib8t01hwq_flt_hlet.fits
16623360 outputs/wfc3/ib8t01010/ib8t01hwq_flt.fits
31901760 outputs/wfc3/ib8t01010/ib8t01htq_raw.fits
4192 outputs/wfc3/ib8t01010/ib8t01hwq.tra
157746240 outputs/wfc3/ib8t01010/ib8t01htq_ima.fits
16623360 outputs/wfc3/ib8t01010/ib8t01htq_flt.fits
8640 outputs/wfc3/ib8t01010/ib8t01htq_flt_hlet.fits
12654720 outputs/wfc3/ib8t01010/ib8t01010_drz.fits
4733 outputs/wfc3/ib8t01010/ib8t01htq.tra
11520 outputs/wfc3/ib8t01010/ib8t01010_asn.fits
16110 outputs/wfc3/ib8t01010/ib8t01010.tra
        """,
    ),
]


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
    # List files from all modes,  they'll be nothing for inapplicable modes.
    # Choose files from print to define truth values for future tests.
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
    return pipe(f"/usr/bin/find {path} -type f", "xargs ls -lt", "awk -e {print($5,$9);}")


def list_s3(path):
    """List S3 files at `path` for defining truth data and actual files."""
    output = pipe(f"aws s3 ls --recursive {path}", "awk -e {print($3,$4);}")
    return output.replace(" ", " outputs/")


def pipe(*args, encoding="utf-8", print_output=False, raise_exception=False):
    """Every arg should be a subprocess command string which will be run and piped to
    any subsequent args in a linear process chain.  Each arg will be split into command
    words based on whitespace so whitespace embedded within words is not possible.

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
    ret_code = p.wait()
    if ret_code and raise_exception:
        raise RuntimeError(f"Subprocess failed with with status: {ret_code}")
    output = output.decode(encoding) if encoding else output
    if print_output:
        print(output, end="")
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
            continue  # astroquery inputs are irrelevant.  currently 'file:' inputs double as outputs.
        assert expected_name in actual_files
        if expected_name.startswith("s3://"):
            continue
        assert (
            abs(actual_files[expected_name] - expected_size) < CALDP_TEST_FILE_SIZE_THRESHOLD * expected_size
        ), "bad size for " + repr(expected_name)
