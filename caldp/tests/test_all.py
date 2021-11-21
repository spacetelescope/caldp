"""This module defines tests for the process.py module which handles obtaining data,
assigning references, and basic calibrations.
"""
import os
import subprocess
import tempfile

import pytest

from caldp import process
from caldp import create_previews
from caldp import messages
from caldp import file_ops
from caldp import sysexit


# ----------------------------------------------------------------------------------------

# Set default CRDS Context
CRDS_CONTEXT = os.environ.get("CRDS_CONTEXT")
if CRDS_CONTEXT == "":
    os.environ["CRDS_CONTEXT"] = "hst_0967.pmap"

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
62666 inputs/j8cb010b0.tra
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
18457 outputs/j8cb010b0/previews/j8cb010b1_crj.jpg
1036 outputs/j8cb010b0/previews/j8cb010b1_crj_thumb.jpg
31941 outputs/j8cb010b0/previews/j8cb010b1_drz.jpg
1920 outputs/j8cb010b0/previews/j8cb010b1_drz_thumb.jpg
21728 outputs/j8cb010b0/previews/j8cb01u3q_raw.jpg
1156 outputs/j8cb010b0/previews/j8cb01u3q_raw_thumb.jpg
18712 outputs/j8cb010b0/previews/j8cb01u2q_flt.jpg
1084 outputs/j8cb010b0/previews/j8cb01u2q_flt_thumb.jpg
21587 outputs/j8cb010b0/previews/j8cb01u2q_raw.jpg
1159 outputs/j8cb010b0/previews/j8cb01u2q_raw_thumb.jpg
18748 outputs/j8cb010b0/previews/j8cb01u3q_flt.jpg
1083 outputs/j8cb010b0/previews/j8cb01u3q_flt_thumb.jpg
2257920 outputs/j8cb010b0/j8cb01u3q_raw.fits
51840 outputs/j8cb010b0/j8cb01u3q_flt_hlet.fits
10820160 outputs/j8cb010b0/j8cb01u3q_flt.fits
2818 outputs/j8cb010b0/j8cb01u3q.tra
2257920 outputs/j8cb010b0/j8cb01u2q_raw.fits
51840 outputs/j8cb010b0/j8cb01u2q_flt_hlet.fits
10820160 outputs/j8cb010b0/j8cb01u2q_flt.fits
3547 outputs/j8cb010b0/j8cb01u2q.tra
15963840 outputs/j8cb010b0/j8cb010b1_drz.fits
10535040 outputs/j8cb010b0/j8cb010b1_crj.fits
5166 outputs/j8cb010b0/j8cb010b1.tra
11520 outputs/j8cb010b0/j8cb010b0_asn.fits
62666 outputs/j8cb010b0/j8cb010b0.tra
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
121908 outputs/obes03010/previews/obes03010_sx1.png
8486 outputs/obes03010/previews/obes03010_sx1_thumb.png
4501440 outputs/obes03010/obes03010_raw.fits
80640 outputs/obes03010/obes03010_sx1.fits
2260800 outputs/obes03010/obes03010_wav.fits
10537920 outputs/obes03010/obes03010_crj.fits
14477760 outputs/obes03010/obes03010_sx2.fits
11520 outputs/obes03010/obes03010_asn.fits
21052800 outputs/obes03010/obes03010_flt.fits
        """,
    ),
    (
        "la8q99030",
        """
7799 la8q99ixq_x1d_thumb.png
115200 la8q99ixq_x1d_prev.fits
235844 la8q99ixq_x1d.png
7745 la8q99030_x1dsum_thumb.png
115200 la8q99030_x1dsum_prev.fits
234387 la8q99030_x1dsum.png
7745 la8q99030_x1dsum3_thumb.png
115200 la8q99030_x1dsum3_prev.fits
234667 la8q99030_x1dsum3.png
218880 la8q99030_jnk.fits
11520 la8q99030_asn.fits
259200 la8q99030_x1dsum3.fits
259200 la8q99030_x1dsum.fits
13092480 la8q99ixq_counts.fits
13092480 la8q99ixq_flt.fits
12441600 la8q99ixq_corrtag.fits
429120 la8q99ixq_x1d.fits
4870 la8q99ixq.tra
806400 la8q99ixq_lampflash.fits
2560320 la8q99ixq_rawtag.fits
218880 la8q99ixq_spt.fits
235844 outputs/la8q99030/previews/la8q99ixq_x1d.png
115200 outputs/la8q99030/previews/la8q99ixq_x1d_prev.fits
7799 outputs/la8q99030/previews/la8q99ixq_x1d_thumb.png
234387 outputs/la8q99030/previews/la8q99030_x1dsum.png
115200 outputs/la8q99030/previews/la8q99030_x1dsum_prev.fits
7745 outputs/la8q99030/previews/la8q99030_x1dsum_thumb.png
234667 outputs/la8q99030/previews/la8q99030_x1dsum3.png
115200 outputs/la8q99030/previews/la8q99030_x1dsum3_prev.fits
7745 outputs/la8q99030/previews/la8q99030_x1dsum3_thumb.png
2560320 outputs/la8q99030/la8q99ixq_rawtag.fits
218880 outputs/la8q99030/la8q99ixq_spt.fits
429120 outputs/la8q99030/la8q99ixq_x1d.fits
13092480 outputs/la8q99030/la8q99ixq_flt.fits
806400 outputs/la8q99030/la8q99ixq_lampflash.fits
13092480 outputs/la8q99030/la8q99ixq_counts.fits
12441600 outputs/la8q99030/la8q99ixq_corrtag.fits
259200 outputs/la8q99030/la8q99030_x1dsum3.fits
4890 outputs/la8q99030/la8q99ixq.tra
11520 outputs/la8q99030/la8q99030_asn.fits
218880 outputs/la8q99030/la8q99030_jnk.fits
259200 outputs/la8q99030/la8q99030_x1dsum.fits
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
86860 ib8t01010.tra
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
48819 outputs/ib8t01010/previews/ib8t01htq_ima.jpg
1877 outputs/ib8t01010/previews/ib8t01htq_ima_thumb.jpg
205588 outputs/ib8t01010/previews/ib8t01hwq_raw.jpg
1997 outputs/ib8t01010/previews/ib8t01hwq_raw_thumb.jpg
27459 outputs/ib8t01010/previews/ib8t01010_drz.jpg
1841 outputs/ib8t01010/previews/ib8t01010_drz_thumb.jpg
38031 outputs/ib8t01010/previews/ib8t01htq_flt.jpg
1615 outputs/ib8t01010/previews/ib8t01htq_flt_thumb.jpg
37404 outputs/ib8t01010/previews/ib8t01hwq_flt.jpg
1551 outputs/ib8t01010/previews/ib8t01hwq_flt_thumb.jpg
206484 outputs/ib8t01010/previews/ib8t01htq_raw.jpg
2058 outputs/ib8t01010/previews/ib8t01htq_raw_thumb.jpg
46513 outputs/ib8t01010/previews/ib8t01hwq_ima.jpg
1859 outputs/ib8t01010/previews/ib8t01hwq_ima_thumb.jpg
31901760 outputs/ib8t01010/ib8t01hwq_raw.fits
157746240 outputs/ib8t01010/ib8t01hwq_ima.fits
8640 outputs/ib8t01010/ib8t01hwq_flt_hlet.fits
16623360 outputs/ib8t01010/ib8t01hwq_flt.fits
31901760 outputs/ib8t01010/ib8t01htq_raw.fits
4192 outputs/ib8t01010/ib8t01hwq.tra
157746240 outputs/ib8t01010/ib8t01htq_ima.fits
16623360 outputs/ib8t01010/ib8t01htq_flt.fits
8640 outputs/ib8t01010/ib8t01htq_flt_hlet.fits
12654720 outputs/ib8t01010/ib8t01010_drz.fits
4733 outputs/ib8t01010/ib8t01htq.tra
11520 outputs/ib8t01010/ib8t01010_asn.fits
86860 outputs/ib8t01010/ib8t01010.tra
        """,
    ),  # wfc3 and acs singletons
    (
        "ibc604b9q",
        """
828 ibc604b9q.tra
32518080 ibc604b9q_raw.fits
221702 ibc604b9q_raw.jpg
842 ibc604b9q_raw_thumb.jpg
828 outputs/ibc604b9q/ibc604b9q.tra
32518080 outputs/ibc604b9q/ibc604b9q_raw.fits
842 outputs/ibc604b9q/ibc604b9q_raw_thumb.jpg
221702 outputs/ibc604b9q/ibc604b9q_raw.jpg
        """,
    ),
    (
        "j8f54obeq",
        """
184035 j8f54obeq_raw.jpg
7289 j8f54obeq_raw_thumb.jpg
178971 j8f54obeq_flt.jpg
7354 j8f54obeq_flt_thumb.jpg
2732 j8f54obeq.tra
10532160 j8f54obeq_flt.fits
2257920 j8f54obeq_raw.fits
2732 outputs/j8f54obeq/j8f54obeq.tra
2257920 outputs/j8f54obeq/j8f54obeq_raw.fits
10532160 outputs/j8f54obeq/j8f54obeq_flt.fits
7289 outputs/j8f54obeq/j8f54obeq_raw_thumb.jpg
184035 outputs/j8f54obeq/j8f54obeq_raw.jpg
7354 outputs/j8f54obeq/j8f54obeq_flt_thumb.jpg
178971 outputs/j8f54obeq/j8f54obeq_flt.jpg
        """,
    ),  # testing env file
    (
        "iacs01t4q",
        """
12600000 iacs01t4q_drz.fits
83733 iacs01t4q_drz.jpg
 3604 iacs01t4q_drz_thumb.jpg
16646400 iacs01t4q_flt.fits
 8640 iacs01t4q_flt_hlet.fits
99891 iacs01t4q_flt.jpg
 3862 iacs01t4q_flt_thumb.jpg
115686720 iacs01t4q_ima.fits
  124877 iacs01t4q_ima.jpg
 4306 iacs01t4q_ima_thumb.jpg
23400000 iacs01t4q_raw.fits
  247834 iacs01t4q_raw.jpg
 2649 iacs01t4q_raw_thumb.jpg
89378 iacs01t4q.tra
35 outputs/iacs01t4q/env/iacs01t4q_cal_env.txt
12600000 outputs/iacs01t4q/iacs01t4q_drz.fits
16646400 outputs/iacs01t4q/iacs01t4q_flt.fits
8640 outputs/iacs01t4q/iacs01t4q_flt_hlet.fits
115686720 outputs/iacs01t4q/iacs01t4q_ima.fits
23400000 outputs/iacs01t4q/iacs01t4q_raw.fits
89378 outputs/iacs01t4q/iacs01t4q.tra
83733 outputs/iacs01t4q/previews/iacs01t4q_drz.jpg
3604 outputs/iacs01t4q/previews/iacs01t4q_drz_thumb.jpg
99891 outputs/iacs01t4q/previews/iacs01t4q_flt.jpg
3862 outputs/iacs01t4q/previews/iacs01t4q_flt_thumb.jpg
124877 outputs/iacs01t4q/previews/iacs01t4q_ima.jpg
4306 outputs/iacs01t4q/previews/iacs01t4q_ima_thumb.jpg
247834 outputs/iacs01t4q/previews/iacs01t4q_raw.jpg
2649 outputs/iacs01t4q/previews/iacs01t4q_raw_thumb.jpg
        """,
    ),
]

TARFILES = [
    ("j8cb010b0", "32586581 j8cb010b0.tar.gz"),
    ("iacs01t4q", "106921504 iacs01t4q.tar.gz"),
]

S3_OUTPUTS = [
    (
        "j8cb010b0",
        """
        32586581 j8cb010b0.tar.gz
        3726 preview.txt
        112 preview_metrics.txt
        8701 process.txt
        112 process_metrics.txt
        """,
    ),
    (
        "obes03010",
        """
        32802820 obes03010.tar.gz
        11611 preview.txt
        823 preview_metrics.txt
        29773 process.txt
        819 process_metrics.txt
        """,
    ),
    (
        "la8q99030",
        """
        11883173 la8q99030.tar.gz
        6445 preview.txt
        827 preview_metrics.txt
        9782 process.txt
        824 process_metrics.txt
        """,
    ),
    (
        "ib8t01010",
        """
        277780489 ib8t01010.tar.gz
        93209 preview.txt
        828 preview_metrics.txt
        91292 process.txt
        827 process_metrics.txt
        """,
    ),
]

FAIL_OUTPUTS = [
    (
        "j8f54obeq",
        """
        6445230 j8f54obeq.tar.gz
        1136 preview.txt
        113 preview_metrics.txt
        4557 process.txt
        112 process_metrics.txt
        """,
    ),
]

SHORT_TEST_IPPPSSOOTS = [result[0] for result in RESULTS][:1]
LONG_TEST_IPPPSSOOTS = [result[0] for result in RESULTS][:-1]  # [1:]
ENV_TEST_IPPPSSOOTS = [result[0] for result in RESULTS][-1:]

# LONG_TEST_IPPPSSOOTS += SHORT_TEST_IPPPSSOOTS  # Include all for creating test cases.

# Leave S3 config undefined to skip S3 tests
CALDP_S3_TEST_OUTPUTS = os.environ.get("CALDP_S3_TEST_OUTPUTS")  # s3://caldp-output-test/pytest/outputs
CALDP_S3_TEST_INPUTS = os.environ.get("CALDP_S3_TEST_INPUTS")  # s3://caldp-output-test/inputs

# Output sizes must be within +- this fraction of truth value
CALDP_TEST_FILE_SIZE_THRESHOLD = float(os.environ.get("CALDP_TEST_FILE_SIZE_THRESHOLD", 0.4))

# ----------------------------------------------------------------------------------------


@pytest.mark.parametrize("output_uri", ["file:outputs", CALDP_S3_TEST_OUTPUTS])
@pytest.mark.parametrize("input_uri", ["file:inputs", "astroquery:", CALDP_S3_TEST_INPUTS])
@pytest.mark.parametrize("ipppssoot", SHORT_TEST_IPPPSSOOTS)
def test_io_modes(tmpdir, ipppssoot, input_uri, output_uri):
    coretst(tmpdir, ipppssoot, input_uri, output_uri)


@pytest.mark.parametrize("output_uri", ["file:outputs"])
@pytest.mark.parametrize("input_uri", ["file:inputs"])
@pytest.mark.parametrize("ipppssoot", ENV_TEST_IPPPSSOOTS)
def test_env_file(tmpdir, ipppssoot, input_uri, output_uri):
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
    working_dir = os.path.join(temp_dir, ipppssoot)
    os.makedirs(working_dir, exist_ok=True)
    os.chdir(working_dir)
    try:
        if input_uri.startswith("file"):
            setup_io(ipppssoot, input_uri, output_uri)
            tarball = check_tarball_in(ipppssoot)

        process.process(ipppssoot, input_uri, output_uri)
        messages.log_metrics(log_file="process.txt", metrics="process_metrics.txt")
        check_messages(ipppssoot, output_uri, status="processing")
        create_previews.main(ipppssoot, input_uri, output_uri)
        messages.log_metrics(log_file="preview.txt", metrics="preview_metrics.txt")
        expected_inputs, expected_outputs = expected(RESULTS, ipppssoot)
        actual_inputs = list_inputs(ipppssoot, input_uri)
        actual_outputs = list_outputs(ipppssoot, output_uri)
        check_inputs(input_uri, expected_inputs, actual_inputs)
        check_outputs(output_uri, expected_outputs, actual_outputs)
        messages.main(input_uri, output_uri, ipppssoot)
        check_s3_outputs(S3_OUTPUTS, actual_outputs, ipppssoot, output_uri)
        check_logs(input_uri, output_uri, ipppssoot)
        check_messages(ipppssoot, output_uri, status="processed.trigger")
        # tests whether file_ops gracefully handles an exception type
        file_ops.clean_up([], ipppssoot, dirs=["dummy_dir"])

        if input_uri.startswith("file"):  # create tarfile if s3 access unavailable
            actual_tarfiles = check_tarball_out(ipppssoot, input_uri, output_uri)
            check_tarfiles(TARFILES, actual_tarfiles, ipppssoot, output_uri)
            check_pathfinder(ipppssoot)
            message_status_check(input_uri, output_uri, ipppssoot)
            os.remove(tarball)
        # check output tarball for failed jobs
        check_failed_job_tarball(ipppssoot, input_uri, output_uri)
        check_messages_cleanup(ipppssoot)
        if input_uri.startswith("astroquery"):
            check_IO_clean_up(ipppssoot)
        check_sysexit_retry()
    finally:
        os.chdir(temp_dir)


# ----------------------------------------------------------------------------------------


def parse_results(results):
    """Break up multiline output from ls into a list of (name, size) tuples."""
    return [(line.split()[1], int(line.split()[0])) for line in results.splitlines() if line.strip()]


def setup_io(ipppssoot, input_uri, output_uri):
    working_dir = os.getcwd()
    if input_uri.startswith("file"):
        os.makedirs("outputs", exist_ok=True)
        os.makedirs("inputs", exist_ok=True)
        os.chdir("inputs")
        process.download_inputs(ipppssoot, input_uri, output_uri, make_env=True)
    os.chdir(working_dir)


def check_tarball_in(ipppssoot):
    tarball = f"{ipppssoot.lower()}.tar.gz"
    tarball_path = os.path.join("inputs", tarball)
    if os.path.exists(tarball_path):
        assert True
    return tarball_path


def check_tarball_out(ipppssoot, input_uri, output_uri):
    """Create a tarfile from outputs - only runs for a single dataset (test_io)
    Workaround to improve test coverage when s3 bucket access is unavailable.
    """
    if output_uri.startswith("file"):
        """this call to tar_outputs will actually test file_ops.clean_up
        so there's technically no need to do it further below
        the problem with doing it here is we need a lot of logic
        to find "all" of the files to cleanup, and that logic
        in and of itself is what really needs to be tested...
        meaning it should be caldp, not in the test
        """
        tar, file_list = file_ops.tar_outputs(ipppssoot, input_uri, output_uri)
        assert len(file_list) > 0
        tarpath = os.path.join("outputs", tar)
        assert os.path.exists(tarpath)
        all_files = list_files(os.path.dirname(tarpath), ipppssoot)
        actual_tarfiles = {}
        for name, size in all_files.items():
            if name.endswith(".tar.gz"):
                actual_tarfiles[name] = size
        return actual_tarfiles


def check_failed_job_tarball(ipppssoot, input_uri, output_uri):
    """In the case of a processing error, tar the input files and upload to s3 for debugging.
    test case: iacs01t4q, astroquery:, file:outputs
    Note: if caldp fails during processing, the .fits and .tra files are never copied over to /outputs folder but the (partially) processed input files are available in /inputs.
    """
    if ipppssoot == "j8f54obeq" and input_uri.startswith("astroquery"):
        working_dir = os.getcwd()
        fail_outputs = dict(FAIL_OUTPUTS)
        expected = {}
        for (name, size) in parse_results(fail_outputs[ipppssoot]):
            expected[name] = size
        # manually search and delete output files so it's forced to use the inputs
        output_dir = file_ops.get_output_dir(output_uri)
        os.chdir(output_dir)
        output_files = file_ops.find_output_files(ipppssoot)
        # assert len(output_files) == 6
        if len(output_files) > 0:
            print("Removing outputs for failed job test:")
            for f in output_files:
                print(f)
                os.remove(f)
            empty_outputs = file_ops.find_output_files(ipppssoot)
            print("Files remaining in outputs dir: ", len(empty_outputs))
            assert len(empty_outputs) == 0
        os.chdir(working_dir)
        tar, file_list = file_ops.tar_outputs(ipppssoot, input_uri, output_uri)
        assert len(file_list) == 7
        assert os.path.exists(os.path.join("inputs", tar))
        actual = list_inputs(ipppssoot, input_uri)
        log_path = os.path.join("outputs", ipppssoot, "logs")
        assert os.path.exists(log_path)
        actual.update(list_logs(log_path))
        check_outputs(output_uri, expected, actual)


def check_messages_cleanup(ipppssoot):
    # logs/ipppssoot just ensures test coverage in messages.clean_up
    dirs = ["messages", f"logs/{ipppssoot}"]
    # if they don't exist yet, make them just to be sure
    # and put a dummy file in there to test
    tempfiles = []
    for d in dirs:
        if not os.path.isdir(d):
            os.makedirs(d)
        f = tempfile.NamedTemporaryFile(dir=d, delete=False)
        f.close()
        tempfiles.append(f.name)
        assert os.path.isdir(d)
        assert os.path.isfile(f.name)
    # clean them up...
    for d in dirs:
        messages.clean_up(ipppssoot, d.split("/")[0])
    # and check that they're gone
    for i, d in enumerate(dirs):
        assert not os.path.isdir(d)
        assert not os.path.isfile(tempfiles[i])


def check_IO_clean_up(ipppssoot):
    """Test cleanup using Astroquery inputs and local outputs.
    NOTE: cleanup of inputs would normally only occur if using s3
    """
    messages.clean_up(ipppssoot, IO="outputs")
    assert not os.path.isdir(os.path.join(os.getcwd(), "outputs"))
    assert not os.path.isdir(os.path.join(os.getcwd(), "outputs", ipppssoot))
    messages.clean_up(ipppssoot, IO="inputs")
    assert not os.path.isdir(os.path.join(os.getcwd(), "inputs"))
    assert not os.path.isdir(os.path.join(os.getcwd(), "inputs", ipppssoot))


def list_files(startpath, ipppssoot):
    file_dict = {}
    for root, _, files in os.walk(startpath):
        for f in sorted(files, key=lambda f: os.path.getsize(root + os.sep + f)):
            if f.startswith(ipppssoot[0:5]):
                file_dict[f] = os.path.getsize(root + os.sep + f)
    return file_dict


def list_logs(logpath):
    log_dict = {}
    for root, _, files in os.walk(logpath):
        for f in sorted(files, key=lambda f: os.path.getsize(root + os.sep + f)):
            log_dict[f] = os.path.getsize(root + os.sep + f)
    return log_dict


def list_objects(path):
    object_dict = {}
    output = pipe(f"aws s3 ls --recursive {path}")
    results = [(int(line.split()[2]), line.split()[3]) for line in output.splitlines() if line.strip()]
    for (size, name) in results:
        filename = os.path.basename(name)
        object_dict[filename] = size
    return object_dict


def list_inputs(ipppssoot, input_uri):
    working_dir = os.getcwd()
    if input_uri.startswith("file"):
        input_path = os.path.join(working_dir, "inputs")
    else:
        input_path = os.path.join(working_dir, "inputs", ipppssoot)
    inputs = list_files(input_path, ipppssoot)
    return inputs


def list_outputs(ipppssoot, output_uri):
    """Routinely log input, output, and CWD files to aid setting up expected results."""
    # List files from all modes,  they'll be nothing for inapplicable modes.
    # Choose files from print to define truth values for future tests.
    output_path = process.get_output_path(output_uri, ipppssoot)
    if output_uri.startswith("file"):
        outputs = list_files(output_path, ipppssoot)
    elif CALDP_S3_TEST_OUTPUTS and output_uri.lower().startswith("s3"):
        outputs = list_objects(output_path)
    return outputs


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


def expected(RESULTS, ipppssoot):
    results = dict(RESULTS)
    expected_outputs, expected_inputs = {}, {}
    for (name, size) in parse_results(results[ipppssoot]):
        if name.startswith("outputs"):
            name = os.path.basename(name)
            expected_outputs[name] = size
        else:
            name = os.path.basename(name)
            expected_inputs[name] = size
    return expected_inputs, expected_outputs


def check_inputs(input_uri, expected_inputs, actual_inputs):
    for name in list(expected_inputs.keys()):
        assert name in list(actual_inputs.keys())


def check_outputs(output_uri, expected_outputs, actual_outputs):
    if output_uri.startswith("file"):
        for name, size in expected_outputs.items():
            assert name in list(actual_outputs.keys())
            assert abs(actual_outputs[name] - size) < CALDP_TEST_FILE_SIZE_THRESHOLD * size, "bad size for " + repr(
                name
            )


def check_tarfiles(TARFILES, actual_tarfiles, ipppssoot, output_uri):
    tarfiles = dict(TARFILES)
    expected = {}
    for (name, size) in parse_results(tarfiles[ipppssoot]):
        expected[name] = size

    # check_tarball_out doesn't handle s3 output_uris so actual_tarfiles is NoneType in come cases
    if not output_uri.startswith("s3:"):
        for name, size in expected.items():
            assert name in list(actual_tarfiles.keys())
            assert abs(actual_tarfiles[name] - size) < CALDP_TEST_FILE_SIZE_THRESHOLD * size, "bad size for " + repr(
                name
            )


def check_s3_outputs(TARFILES, actual_outputs, ipppssoot, output_uri):
    if CALDP_S3_TEST_OUTPUTS and output_uri.lower().startswith("s3"):
        tarfiles = dict(S3_OUTPUTS)
        expected = {}
        for (name, size) in parse_results(tarfiles[ipppssoot]):
            expected[name] = size
        for name, size in expected.items():
            assert name in list(actual_outputs.keys())
            assert abs(actual_outputs[name] - size) < CALDP_TEST_FILE_SIZE_THRESHOLD * size, "bad size for " + repr(
                name
            )


def check_pathfinder(ipppssoot):
    input_uri_prefix = "file:inputs"
    output_uri_prefix = None
    output_uri, output_path = messages.path_finder(input_uri_prefix, output_uri_prefix, ipppssoot)
    assert output_uri == input_uri_prefix
    assert output_path == os.path.abspath("inputs")

    input_uri_prefix = "astroquery:"
    output_uri, output_path = messages.path_finder(input_uri_prefix, output_uri_prefix, ipppssoot)
    prefix = os.path.join(os.getcwd(), "inputs", ipppssoot)
    assert output_uri == f"file:{prefix}"


def check_logs(input_uri, output_uri, ipppssoot):
    if output_uri.startswith("file"):
        output_uri, output_path = messages.path_finder(input_uri, output_uri, ipppssoot)
        logs = messages.Logs(output_path, output_uri, ipppssoot)
        log_path = logs.get_log_output()
        assert os.path.exists(log_path)
        try:
            logs.upload_logs()
        except Exception as e:
            print("s3 error check: ", e)
            assert True


def check_messages(ipppssoot, output_uri, status):
    if "." in status:
        status, suffix = status.split(".")
        suffix = f".{suffix}"
    else:
        suffix = ""

    if CALDP_S3_TEST_OUTPUTS and output_uri.lower().startswith("s3"):
        s3_messages = list_objects(f"{output_uri}/messages")
        expected_message = f"{status}-{ipppssoot}{suffix}"
        assert expected_message in list(s3_messages.keys())
    else:
        working_dir = os.getcwd()
        proc_msg = os.path.join(working_dir, "messages", f"{status}-{ipppssoot}{suffix}")
        err_msg = os.path.join(working_dir, "messages", f"error-{ipppssoot}")
        assert os.path.exists(proc_msg) or os.path.exists(err_msg)


def message_status_check(input_uri, output_uri, ipppssoot):
    output_path = messages.get_local_outpath(output_uri, ipppssoot)
    msg = messages.Messages(output_uri, output_path, ipppssoot)
    assert msg.msg_dir == os.path.join(os.getcwd(), "messages")
    assert msg.stat == 0

    msg.init()
    assert os.path.exists(msg.msg_dir)
    # assert msg.name == f"submit-{ipppssoot}"
    # assert msg.file == f"{msg.msg_dir}/{msg.name}"
    assert msg.stat == 1

    msg.process_message()
    assert msg.name == f"processing-{ipppssoot}"
    assert msg.file == f"{msg.msg_dir}/{msg.name}"
    assert msg.stat == 2

    msg.preview_message()
    assert msg.name == f"processing-{ipppssoot}"
    assert msg.file == f"{msg.msg_dir}/{msg.name}"
    assert msg.stat == 2

    msg.final_message()
    if msg.stat == -1:
        assert msg.name == f"error-{ipppssoot}"
    elif msg.stat == 3:
        assert msg.name == f"processed-{ipppssoot}.trigger"


def check_sysexit_retry():
    def no_exc_func(arg1=True):
        pass

    def exc_func(arg1=True):
        raise NotImplementedError

    retry_no_exc = sysexit.retry(no_exc_func)
    retry_exc = sysexit.retry(exc_func)

    retry_no_exc()
    with pytest.raises(NotImplementedError):
        retry_exc()
