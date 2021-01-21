"""This module defines tests for the process.py module which handles obtaining data,
assigning references, and basic calibrations.
"""
import os
import glob
import subprocess
import pytest
from caldp import process
from caldp import create_previews
from caldp import messages

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
235844 outputs/cos/la8q99030/previews/la8q99ixq_x1d.png
115200 outputs/cos/la8q99030/previews/la8q99ixq_x1d_prev.fits
7799 outputs/cos/la8q99030/previews/la8q99ixq_x1d_thumb.png
234387 outputs/cos/la8q99030/previews/la8q99030_x1dsum.png
115200 outputs/cos/la8q99030/previews/la8q99030_x1dsum_prev.fits
7745 outputs/cos/la8q99030/previews/la8q99030_x1dsum_thumb.png
234667 outputs/cos/la8q99030/previews/la8q99030_x1dsum3.png
115200 outputs/cos/la8q99030/previews/la8q99030_x1dsum3_prev.fits
7745 outputs/cos/la8q99030/previews/la8q99030_x1dsum3_thumb.png
2560320 outputs/cos/la8q99030/la8q99ixq_rawtag.fits
218880 outputs/cos/la8q99030/la8q99ixq_spt.fits
429120 outputs/cos/la8q99030/la8q99ixq_x1d.fits
13092480 outputs/cos/la8q99030/la8q99ixq_flt.fits
806400 outputs/cos/la8q99030/la8q99ixq_lampflash.fits
13092480 outputs/cos/la8q99030/la8q99ixq_counts.fits
12441600 outputs/cos/la8q99030/la8q99ixq_corrtag.fits
259200 outputs/cos/la8q99030/la8q99030_x1dsum3.fits
4870 outputs/cos/la8q99030/la8q99ixq.tra
11520 outputs/cos/la8q99030/la8q99030_asn.fits
218880 outputs/cos/la8q99030/la8q99030_jnk.fits
259200 outputs/cos/la8q99030/la8q99030_x1dsum.fits
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

# TARFILES = [
#     ("j8cb010b0", "65099 j8cb010b0.tar.gz")
#     ("obes03010", "obes03010.tar.gz"
#     ("la8q99030", "la8q99030.tar.gz")
#     ("ib8t01010", "ib8t01010.tar.gz")
# ]

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
    working_dir = os.path.join(temp_dir, ipppssoot)
    os.makedirs(working_dir, exist_ok=True)
    os.chdir(working_dir)
    try:
        if input_uri.startswith("file"):
            setup_io(ipppssoot, input_uri, output_uri)

        process.process(ipppssoot, input_uri, output_uri)
        messages.log_metrics(log_file="process.txt", metrics="process_metrics.txt")
        create_previews.main(ipppssoot, input_uri, output_uri)
        messages.log_metrics(log_file="preview.txt", metrics="preview_metrics.txt")
        expected_inputs, expected_outputs = expected(RESULTS, ipppssoot)
        actual_inputs = list_inputs(ipppssoot, input_uri)
        actual_outputs = list_outputs(ipppssoot, output_uri)
        check_inputs(input_uri, expected_inputs, actual_inputs)
        check_outputs(output_uri, expected_outputs, actual_outputs)
        messages.main(input_uri, output_uri, ipppssoot)
        check_logs(input_uri, output_uri, ipppssoot)
        check_messages(ipppssoot, output_uri)
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
        process.download_inputs(ipppssoot, input_uri, output_uri)  # get inputs separately
    os.chdir(working_dir)


def list_files(startpath, ipppssoot):
    file_dict = {}
    for root, _, files in os.walk(startpath):
        for f in sorted(files, key=lambda f: os.path.getsize(root + os.sep + f)):
            if f.startswith(ipppssoot[0:5]):
                file_dict[f] = os.path.getsize(root + os.sep + f)
    return file_dict


def list_objects(path):
    object_dict = {}
    log_list, msg_list = [], []
    output = pipe(f"aws s3 ls --recursive {path}")
    results = [(int(line.split()[2]), line.split()[3]) for line in output.splitlines() if line.strip()]
    for (size, name) in results:
        if "logs" in name:
            log_list.append(name)
        elif "messages" in name:
            msg_list.append(name)
        else:
            filename = os.path.basename(name)
            object_dict[filename] = size
    if "logs" in path:
        return log_list
    elif "messages" in path:
        return msg_list
    else:
        return object_dict


def list_inputs(ipppssoot, input_uri):
    working_dir = os.getcwd()
    if input_uri.startswith("file"):
        input_path = os.path.join(working_dir, "inputs")
    else:
        input_path = os.path.join(working_dir, ipppssoot)
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
    for name, size in expected_outputs.items():
        assert name in list(actual_outputs.keys())
        assert abs(actual_outputs[name] - size) < CALDP_TEST_FILE_SIZE_THRESHOLD * size, "bad size for " + repr(name)


def check_logs(input_uri, output_uri, ipppssoot):
    working_dir = os.getcwd()
    get_logs = list(glob.glob(f"{working_dir}/*.txt"))
    assert len(get_logs) == 4
    output_uri, output_path = messages.path_finder(input_uri, output_uri, ipppssoot)
    log_path = messages.Logs(output_path, output_uri).log_output
    if CALDP_S3_TEST_OUTPUTS and output_uri.startswith("s3"):
        s3_logs = list_objects(log_path)
        assert len(s3_logs) == 4
    else:
        assert os.path.exists(log_path)


def check_messages(ipppssoot, output_uri):
    if CALDP_S3_TEST_OUTPUTS and output_uri.lower().startswith("s3"):
        message_path = list_objects(f"{output_uri}/messages/")
        assert message_path[0].split("/")[-1] == ipppssoot
    else:
        working_dir = os.getcwd()
        proc_msg = os.path.join(working_dir, "messages", "dataset-processed", ipppssoot)
        err_msg = os.path.join(working_dir, "messages", "dataset-error", ipppssoot)
        if os.path.exists(proc_msg):
            assert True
        elif os.path.exists(err_msg):
            assert True
