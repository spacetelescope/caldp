import os
import glob

import pytest

from caldp import process

IPPPSSOOTS = """
j8cb010b0
ldqhpbi9q
o8l7sws9q
octka6010
""".split()

RESULTS = {
    "j8cb010b0" : """
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
"""
}

def parse_results(results):
    return [
        (line.split()[1], int(line.split()[0]))
        for line in results.splitlines()
        if line.strip()
    ]

def check_results(ipppssoot):
    expected = parse_results(RESULTS[ipppssoot])
    working_files = {
        os.path.basename(path): os.stat(path).st_size
        for path in glob.glob("*")
    }
    for (expected_name, expected_size) in expected:
        assert expected_name in working_files
        assert abs(working_files[expected_name]-expected_size) < 0.1*expected_size

@pytest.mark.parametrize("ipppssoot", ["j8cb010b0"])
def test_ipppssoot(tmpdir, ipppssoot):
    working_dir = tmpdir.mkdir(ipppssoot)
    old_dir = working_dir.chdir()
    try:
        process.process(ipppssoot, "astroquery:", "file:outputs/"+ipppssoot)
        check_results(ipppssoot)
    finally:
        old_dir.chdir()
