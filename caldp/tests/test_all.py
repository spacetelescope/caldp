"""This module defines tests for the process.py module which handles obtaining data,
assigning references, and basic calibrations.
"""

import os
import tempfile

import pytest

from caldp import process
from caldp import create_previews
from caldp import messages
from caldp import file_ops
from caldp import sysexit

from moto import mock_aws

# ----------------------------------------------------------------------------------------

# Set default CRDS Context
CRDS_CONTEXT = os.environ.get("CRDS_CONTEXT")
if CRDS_CONTEXT == "":
    os.environ["CRDS_CONTEXT"] = "hst_1169.pmap"

# For applicable tests,  the product files associated with each ipppssoot below
# must be present in the CWD after processing and be within 10% of the listed sizes.
RESULTS = dict(
    [
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
48082 iacs01t4q.tra
35 outputs/iacs01t4q/env/iacs01t4q_cal_env.txt
12600000 outputs/iacs01t4q/iacs01t4q_drz.fits
16646400 outputs/iacs01t4q/iacs01t4q_flt.fits
8640 outputs/iacs01t4q/iacs01t4q_flt_hlet.fits
115686720 outputs/iacs01t4q/iacs01t4q_ima.fits
23400000 outputs/iacs01t4q/iacs01t4q_raw.fits
48082 outputs/iacs01t4q/iacs01t4q.tra
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
)

HAP_RESULTS = dict(
    [
        (
            "skycell-p2381x06y09",
            """
740 inputs/skycell-p2381x06y09_input.out
4613760 inputs/hst_skycell-p2381x06y09_wfc3_uvis_f547m_all_drc.fits
168644160 inputs/hst_13773_20_wfc3_uvis_f657n_icnk20a8_flc.fits
16107 inputs/hst_skycell-p2381x06y09_wfc3_uvis_f547m_all_drc.jpg
168644160 inputs/hst_13773_20_wfc3_uvis_f657n_icnk20af_flc.fits
2568 inputs/hst_skycell-p2381x06y09_wfc3_uvis_f547m_all_drc_thumb.jpg
64590 inputs/hst_skycell-p2381x06y09_wfc3_uvis_f547m_all_trl.txt
3301 inputs/hst_skycell-p2381x06y09_wfc3_uvis_total_all_drc_color_thumb.jpg
4619520 inputs/hst_skycell-p2381x06y09_wfc3_uvis_f657n_all_drc.fits
169148160 inputs/hst_13773_20_wfc3_uvis_f547m_icnk20aw_flc.fits
210 inputs/skycell-p2381x06y09_manifest.txt
168713280 inputs/hst_13773_20_wfc3_uvis_f657n_icnk20ad_flc.fits
2492 inputs/hst_skycell-p2381x06y09_wfc3_uvis_f657n_all_drc_thumb.jpg
169148160 inputs/hst_13773_20_wfc3_uvis_f547m_icnk20ay_flc.fits
17486 inputs/hst_skycell-p2381x06y09_wfc3_uvis_f657n_all_drc.jpg
21579 inputs/hst_skycell-p2381x06y09_wfc3_uvis_total_all_drc_color.jpg
71946 inputs/hst_skycell-p2381x06y09_wfc3_uvis_f657n_all_trl.txt
4613760 outputs/skycell-p2381x06y09/hst_skycell-p2381x06y09_wfc3_uvis_f547m_all_drc.fits
64590 outputs/skycell-p2381x06y09/hst_skycell-p2381x06y09_wfc3_uvis_f547m_all_trl.txt
4619520 outputs/skycell-p2381x06y09/hst_skycell-p2381x06y09_wfc3_uvis_f657n_all_drc.fits
210 outputs/skycell-p2381x06y09/skycell-p2381x06y09_manifest.txt
71946 outputs/skycell-p2381x06y09/hst_skycell-p2381x06y09_wfc3_uvis_f657n_all_trl.txt
16107 outputs/skycell-p2381x06y09/previews/hst_skycell-p2381x06y09_wfc3_uvis_f547m_all_drc.jpg
2568 outputs/skycell-p2381x06y09/previews/hst_skycell-p2381x06y09_wfc3_uvis_f547m_all_drc_thumb.jpg
3301 outputs/skycell-p2381x06y09/previews/hst_skycell-p2381x06y09_wfc3_uvis_total_all_drc_color_thumb.jpg
2492 outputs/skycell-p2381x06y09/previews/hst_skycell-p2381x06y09_wfc3_uvis_f657n_all_drc_thumb.jpg
17486 outputs/skycell-p2381x06y09/previews/hst_skycell-p2381x06y09_wfc3_uvis_f657n_all_drc.jpg
21579 outputs/skycell-p2381x06y09/previews/hst_skycell-p2381x06y09_wfc3_uvis_total_all_drc_color.jpg
        """,
        ),
        (
            "acs_8ph_01",
            """
4848398 inputs/hst_9774_01_acs_wfc_f555w_j8ph01_drc.jpg
5526 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g0_drc_thumb.jpg
7685 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g7_trl.txt
336142080 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g3_drc.fits
5453 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g5_drc_thumb.jpg
1930179 inputs/hst_9774_01_acs_wfc_f555w_j8ph01_segment-cat.ecsv
32787 inputs/hst_9774_01_acs_wfc_j8p_metawcs_all_ref_cat.ecsv
7690 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g5_trl.txt
21024 inputs/hst_9774_01_acs_wfc_f555w_j8ph01_trl.txt
3509517 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g7_drc.jpg
168327360 inputs/j8ph01g0q_flc.fits
336150720 inputs/hst_9774_01_acs_wfc_f814w_j8ph01_drc.fits
168448320 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g9_flc.fits
112320 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g1_hlet.fits
168448320 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g5_flc.fits
3767533 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g5_drc.jpg
5069316 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g0_drc.jpg
112320 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g3_hlet.fits
397493 inputs/hst_9774_01_acs_wfc_total_j8ph01_trl.txt
168327360 inputs/j8ph01g7q_flc.fits
7658 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g1_trl.txt
168448320 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g3_flc.fits
336150720 inputs/hst_9774_01_acs_wfc_f555w_j8ph01_drc.fits
145980 inputs/hst_9774_01_acs_wfc_f814w_j8ph01_point-cat.ecsv
112320 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g9_hlet.fits
7689 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g0_trl.txt
168327360 inputs/j8ph01g1q_flc.fits
112320 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g0_hlet.fits
336142080 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g5_drc.fits
112320 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g5_hlet.fits
4225764 inputs/hst_9774_01_acs_wfc_total_j8ph01_drc.jpg
121418 inputs/hst_9774_01_acs_wfc_total_j8ph01_point-cat.ecsv
443 inputs/acs_8ph_01_input.out
336142080 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g9_drc.fits
3399924 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g1_drc.jpg
4087 inputs/hst_9774_01_acs_wfc_f435w_j8ph01_drc_thumb.jpg
4767 inputs/hst_9774_01_acs_wfc_total_j8ph01_drc_color_thumb.jpg
9288 inputs/astrodrizzle.log
146132 inputs/hst_9774_01_acs_wfc_f555w_j8ph01_point-cat.ecsv
112320 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g7_hlet.fits
4749594 inputs/hst_9774_01_acs_wfc_f435w_j8ph01_drc.jpg
7659 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g9_trl.txt
5117 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g9_drc_thumb.jpg
4092 inputs/hst_9774_01_acs_wfc_total_j8ph01_drc_thumb.jpg
168327360 inputs/j8ph01g9q_flc.fits
3617827 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g3_drc.jpg
4481418 inputs/hst_9774_01_acs_wfc_f814w_j8ph01_drc.jpg
632332 inputs/hst_9774_01_acs_wfc_f814w_j8ph01_segment-cat.ecsv
4099 inputs/hst_9774_01_acs_wfc_f555w_j8ph01_drc_thumb.jpg
336142080 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g0_drc.fits
336142080 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g1_drc.fits
21020 inputs/hst_9774_01_acs_wfc_f435w_j8ph01_trl.txt
2864520 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g9_drc.jpg
336142080 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g7_drc.fits
634886 inputs/hst_9774_01_acs_wfc_f435w_j8ph01_segment-cat.ecsv
7659 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g3_trl.txt
20150 inputs/hst_9774_01_acs_wfc_f814w_j8ph01_trl.txt
168327360 inputs/j8ph01g5q_flc.fits
214045 inputs/hst_9774_01_acs_wfc_total_j8ph01_segment-cat.ecsv
336150720 inputs/hst_9774_01_acs_wfc_f435w_j8ph01_drc.fits
1780 inputs/acs_8ph_01_manifest.txt
5945138 inputs/hst_9774_01_acs_wfc_total_j8ph01_drc_color.jpg
4223 inputs/hst_9774_01_acs_wfc_f814w_j8ph01_drc_thumb.jpg
5532 inputs/hst_9774_01_acs_wfc_f435w_j8ph01g3_drc_thumb.jpg
5148 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g1_drc_thumb.jpg
168448320 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g7_flc.fits
168327360 inputs/j8ph01g3q_flc.fits
146083 inputs/hst_9774_01_acs_wfc_f435w_j8ph01_point-cat.ecsv
168448320 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g1_flc.fits
168448320 inputs/hst_9774_01_acs_wfc_f814w_j8ph01g0_flc.fits
336182400 inputs/hst_9774_01_acs_wfc_total_j8ph01_drc.fits
5476 inputs/hst_9774_01_acs_wfc_f555w_j8ph01g7_drc_thumb.jpg
7685 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01g7_trl.txt
336142080 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01g3_drc.fits
1930179 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01_segment-cat.ecsv
7690 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01g5_trl.txt
21024 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01_trl.txt
336150720 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01_drc.fits
168448320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01g9_flc.fits
112320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01g1_hlet.fits
168448320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01g5_flc.fits
112320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01g3_hlet.fits
397493 outputs/acs_8ph_01/hst_9774_01_acs_wfc_total_j8ph01_trl.txt
7658 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01g1_trl.txt
168448320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01g3_flc.fits
336150720 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01_drc.fits
145980 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01_point-cat.ecsv
112320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01g9_hlet.fits
7689 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01g0_trl.txt
112320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01g0_hlet.fits
336142080 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01g5_drc.fits
112320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01g5_hlet.fits
121418 outputs/acs_8ph_01/hst_9774_01_acs_wfc_total_j8ph01_point-cat.ecsv
336142080 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01g9_drc.fits
9288 outputs/acs_8ph_01/astrodrizzle.log
146132 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01_point-cat.ecsv
112320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01g7_hlet.fits
7659 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01g9_trl.txt
632332 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01_segment-cat.ecsv
336142080 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01g0_drc.fits
336142080 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01g1_drc.fits
21020 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01_trl.txt
336142080 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01g7_drc.fits
634886 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01_segment-cat.ecsv
7659 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01g3_trl.txt
20150 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01_trl.txt
214045 outputs/acs_8ph_01/hst_9774_01_acs_wfc_total_j8ph01_segment-cat.ecsv
336150720 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01_drc.fits
1780 outputs/acs_8ph_01/acs_8ph_01_manifest.txt
168448320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f555w_j8ph01g7_flc.fits
146083 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f435w_j8ph01_point-cat.ecsv
168448320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01g1_flc.fits
168448320 outputs/acs_8ph_01/hst_9774_01_acs_wfc_f814w_j8ph01g0_flc.fits
336182400 outputs/acs_8ph_01/hst_9774_01_acs_wfc_total_j8ph01_drc.fits
4848398 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f555w_j8ph01_drc.jpg
5526 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f814w_j8ph01g0_drc_thumb.jpg
5453 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f435w_j8ph01g5_drc_thumb.jpg
3509517 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f555w_j8ph01g7_drc.jpg
3767533 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f435w_j8ph01g5_drc.jpg
5069316 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f814w_j8ph01g0_drc.jpg
4225764 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_total_j8ph01_drc.jpg
3399924 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f814w_j8ph01g1_drc.jpg
4087 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f435w_j8ph01_drc_thumb.jpg
4767 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_total_j8ph01_drc_color_thumb.jpg
4749594 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f435w_j8ph01_drc.jpg
5117 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f555w_j8ph01g9_drc_thumb.jpg
4092 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_total_j8ph01_drc_thumb.jpg
3617827 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f435w_j8ph01g3_drc.jpg
4481418 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f814w_j8ph01_drc.jpg
4099 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f555w_j8ph01_drc_thumb.jpg
2864520 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f555w_j8ph01g9_drc.jpg
5945138 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_total_j8ph01_drc_color.jpg
4223 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f814w_j8ph01_drc_thumb.jpg
5532 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f435w_j8ph01g3_drc_thumb.jpg
5148 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f814w_j8ph01g1_drc_thumb.jpg
5476 outputs/acs_8ph_01/previews/hst_9774_01_acs_wfc_f555w_j8ph01g7_drc_thumb.jpg
        """,
        ),
    ]
)
TARFILES = dict([("j8cb010b0", "32586581 j8cb010b0.tar.gz"), ("iacs01t4q", "106921504 iacs01t4q.tar.gz")])

S3_OUTPUTS = dict(
    [
        (
            "j8cb010b0",
            """
        32586581 j8cb010b0.tar.gz
        1917 preview.txt
        112 preview_metrics.txt
        59000 process.txt
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
)

FAIL_OUTPUTS = dict(
    [
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
)

SHORT_TEST_IPPPSSOOTS = list(RESULTS.keys())[:1]
LONG_TEST_IPPPSSOOTS = list(RESULTS.keys())[:-1]  # [1:]
ENV_TEST_IPPPSSOOTS = list(RESULTS.keys())[-1:]

# LONG_TEST_IPPPSSOOTS += SHORT_TEST_IPPPSSOOTS  # Include all for creating test cases.

# Leave S3 config undefined to skip S3 tests
CALDP_S3_TEST_OUTPUTS = os.environ.get("CALDP_S3_TEST_OUTPUTS", "s3://caldp-pytest")
CALDP_S3_TEST_INPUTS = os.environ.get("CALDP_S3_TEST_INPUTS", "s3://caldp-pytest/inputs")
CALDP_S3_MOTO = int(os.environ.get("CALDP_S3_MOTO", 1))

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


@pytest.mark.parametrize("output_uri", ["file:outputs"])
@pytest.mark.parametrize("input_uri", ["astroquery:s3://hap-poller-files"])
@pytest.mark.parametrize("dataset", ["skycell-p2381x06y09"])
def test_mvm(dataset, input_uri, output_uri):
    haptst(dataset, input_uri, output_uri)


@pytest.mark.parametrize("output_uri", ["file:outputs"])
@pytest.mark.parametrize("input_uri", ["astroquery:s3://hap-poller-files"])
@pytest.mark.parametrize("dataset", ["acs_8ph_01"])
def test_svm(dataset, input_uri, output_uri):
    haptst(dataset, input_uri, output_uri)


# Conditionally mock S3,  defaulting to mock
mock_aws = mock_aws if CALDP_S3_MOTO else lambda x: x


@mock_aws
def haptst(dataset, input_uri, output_uri):
    """
    Test creating products for SVM and MVM datasets
    Currently only supports input from astroquery downloads and output to local (file:)
    <dataset>_input.out files must be staged in the caldp/tests/ directory
    """

    import boto3

    client = boto3.client("s3")

    test_dir = os.path.join(os.path.expanduser("~"), "caldp/tests")
    hap_poller_file_path = os.path.join(test_dir, f"{dataset}_input.out")
    hap_poller_file_name = f"{dataset}_input.out"

    if input_uri.startswith("astroquery:"):
        input_uri_split = input_uri.split(":")
        if len(input_uri_split) == 3 and input_uri_split[1] == "s3":
            # Upload <dataset>_input.out to S3 if input_uri is astroquery:s3://bucket/prefix
            s3_path = input_uri_split[2].replace("//", "").split("/")
            bucket, prefix = s3_path[0], "/".join(s3_path[1:])
            if prefix:
                objectname = prefix.strip("/") + "/" + hap_poller_file_name
            else:
                objectname = hap_poller_file_name
            client.create_bucket(Bucket=bucket)
            with open(hap_poller_file_path, "rb") as f:
                print("Uploading HAP poller file to S3")
                print("S3 bucket", bucket)
                print("S3 objectname", objectname)
                client.upload_fileobj(f, bucket, objectname)

        process.main(["process.py", input_uri, output_uri, dataset])
        create_previews.main(dataset, input_uri, output_uri)

        expected_inputs, expected_outputs = expected(HAP_RESULTS, dataset)

        input_path = os.path.join(os.getcwd(), "inputs", dataset)
        actual_inputs = list_files_hap(input_path, dataset)
        check_inputs(input_uri, expected_inputs, actual_inputs)

    if output_uri.startswith("file"):
        output_path = process.get_output_path(output_uri, dataset)
        actual_outputs = list_files_hap(output_path, dataset)
        check_outputs(output_uri, expected_outputs, actual_outputs)

    check_messages_cleanup(dataset)
    if input_uri.startswith("astroquery"):
        check_IO_clean_up(dataset)


@mock_aws
def coretst(temp_dir, ipppssoot, input_uri, output_uri):
    """Run every `ipppssoot` through process.process() downloading input files from
    astroquery and writing output files to local storage.   Verify that call to process
    outputs the expected files with reasonable sizes into it's CWD.
    """
    if not input_uri or not output_uri:
        print("Skipping case", ipppssoot, input_uri, output_uri)
        return
    working_dir = os.path.join(temp_dir, ipppssoot)
    os.makedirs(working_dir, exist_ok=True)
    os.chdir(working_dir)
    output_uri = setup_s3(ipppssoot, input_uri, output_uri)
    print("Test case", ipppssoot, input_uri, output_uri)
    try:
        if input_uri.startswith("file"):
            setup_io(ipppssoot, input_uri, output_uri)
            tarball = check_tarball_in(ipppssoot)
        elif input_uri.startswith("s3:"):
            setup_io(ipppssoot, input_uri, output_uri)
            tarball = check_tarball_in(ipppssoot)
            file_ops.upload_tar(tarball, input_uri)
            os.remove(tarball)

        process.main(["process.py", input_uri, output_uri, ipppssoot])

        messages.log_metrics(log_file="process.txt", metrics="process_metrics.txt")

        check_messages(ipppssoot, output_uri, status="processing")

        create_previews.main(ipppssoot, input_uri, output_uri)

        messages.log_metrics(log_file="preview.txt", metrics="preview_metrics.txt")

        expected_inputs, expected_outputs = expected(RESULTS, ipppssoot)
        actual_inputs = list_inputs(ipppssoot, input_uri)
        check_inputs(input_uri, expected_inputs, actual_inputs)

        messages.main(input_uri, output_uri, ipppssoot)
        actual_outputs = list_outputs(ipppssoot, output_uri)

        if output_uri.startswith("file"):
            check_outputs(output_uri, expected_outputs, actual_outputs)
        elif output_uri.lower().startswith("s3"):
            check_s3_outputs(S3_OUTPUTS, actual_outputs, ipppssoot, output_uri)
        check_messages(ipppssoot, output_uri, status="processed.trigger")
        # tests whether file_ops gracefully handles an exception type
        file_ops.clean_up([], ipppssoot, dirs=["dummy_dir"])

        if input_uri.startswith("file"):  # create tarfile if s3 access unavailable
            actual_tarfiles = check_tarball_out(ipppssoot, input_uri, output_uri)
            if not output_uri.startswith("s3:"):
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

# Conditionally set up and tear down moto S3 for AWS simulation


def setup_s3(ipppssoot, input_uri, output_uri):
    if CALDP_S3_MOTO:  # moto doesn't seem to care about duplicate creates
        maybe_create_bucket(input_uri, ipppssoot)
        output_uri = maybe_create_bucket(output_uri, ipppssoot)
    return output_uri


# For S3 uri's,  return the *real* output URI for caldp-process,  not just the bucket
def maybe_create_bucket(uri, ipppssoot):
    import boto3

    if uri.startswith("s3://"):
        bucket = file_ops.s3_split_uri(uri)[0]
        print("Creating bucket:", bucket)
        if bucket:
            boto3.client("s3").create_bucket(
                Bucket=bucket,
                ACL="private",
            )
            return f"{uri}/outputs/{ipppssoot}"
    else:
        return uri


# ----------------------------------------------------------------------------------------


def parse_results(results):
    """Break up multiline output from ls into dict of form:  {filename: size, ...}"""
    return dict([(line.split()[1], int(line.split()[0])) for line in results.splitlines() if line.strip()])


def setup_io(ipppssoot, input_uri, output_uri):
    working_dir = os.getcwd()
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
        expected = parse_results(FAIL_OUTPUTS[ipppssoot])
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
        if output_uri.startswith("file"):
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


def list_files_hap(startpath, dataset):
    file_dict = {}
    dataset_type = process.get_dataset_type(dataset)
    if dataset_type == "svm":
        ipppss = process.get_svm_obs_set(dataset)
        for root, _, files in os.walk(startpath):
            for f in sorted(files, key=lambda f: os.path.getsize(root + os.sep + f)):
                if (
                    f.startswith("hst")
                    or f.__contains__(ipppss)
                    or f.__contains__(dataset)
                    or f.startswith("astrodrizzle.log")
                ):
                    file_dict[f] = os.path.getsize(root + os.sep + f)
    elif dataset_type == "mvm":
        for root, _, files in os.walk(startpath):
            for f in sorted(files, key=lambda f: os.path.getsize(root + os.sep + f)):
                if f.startswith("hst") or f.__contains__(dataset):
                    file_dict[f] = os.path.getsize(root + os.sep + f)
    return file_dict


def list_logs(logpath):
    log_dict = {}
    for root, _, files in os.walk(logpath):
        for f in sorted(files, key=lambda f: os.path.getsize(root + os.sep + f)):
            log_dict[f] = os.path.getsize(root + os.sep + f)
    return log_dict


def list_objects(s3_prefix):
    """Given `s3_prefix` s3 bucket and prefix to list, yield the full
    s3 paths of every object in the associated bucket which match the prefix.
    """
    import boto3

    bucket_name, prefix = file_ops.s3_split_uri(s3_prefix)
    paginator = boto3.client("s3").get_paginator("list_objects_v2")
    config = {"MaxItems": 10**7, "PageSize": 1000}
    rval = {}
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix, PaginationConfig=config):
        for result in page.get("Contents", []):
            if result["Key"]:
                filename = os.path.basename(result["Key"])
                rval[filename] = result["Size"]
    print("list_objects:", s3_prefix, "->", rval)
    return rval


def dump_uri(path):
    list_objects(path)


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
    elif output_uri.lower().startswith("s3"):
        outputs = list_objects(output_path)
    return outputs


def expected(RESULTS, ipppssoot):
    expected_outputs, expected_inputs = {}, {}
    for name, size in parse_results(RESULTS[ipppssoot]).items():
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


def check_tarfiles(TARFILES, actual_tarfiles, ipppssoot, output_uri):
    for name, size in parse_results(TARFILES[ipppssoot]).items():
        assert name in list(actual_tarfiles.keys())
        assert abs(actual_tarfiles[name] - size) < CALDP_TEST_FILE_SIZE_THRESHOLD * size, "bad size for " + repr(name)


def check_s3_outputs(S3_OUTPUTS, actual_outputs, ipppssoot, output_uri):
    for name, size in parse_results(S3_OUTPUTS[ipppssoot]).items():
        assert name in list(actual_outputs.keys())
        assert abs(actual_outputs[name] - size) < CALDP_TEST_FILE_SIZE_THRESHOLD * size, "bad size for " + repr(name)


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


def check_messages(ipppssoot, output_uri, status):
    print("check_messages:", ipppssoot, output_uri, status)
    if "." in status:
        status, suffix = status.split(".")
        suffix = f".{suffix}"
    else:
        suffix = ""

    # output_uri of form:  s3://bucket/outputs/ipppssoot

    if output_uri.lower().startswith("s3"):
        bucket, key = file_ops.s3_split_uri(output_uri)
        dump_uri(f"s3://{bucket}")
        dump_uri(f"s3://{bucket}/inputs/")
        dump_uri(f"s3://{bucket}/outputs/")
        dump_uri(f"s3://{bucket}/messages/")
        s3_messages = list_objects(f"s3://{bucket}/messages/")
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
