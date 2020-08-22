"""This module integrates basic calibration processing and preview generation,
initially for the purpose of supporting testing.
"""

from caldp import process
from caldp import create_previews


def main(ipppssoot, input_uri, output_uri):
    process.process(ipppssoot, input_uri, output_uri)
    preview_input_uri = "file:." if input_uri.startswith("astroquery:") else input_uri
    preview_output_uri = process.get_output_path(output_uri, ipppssoot) + "/previews"
    create_previews.main(preview_input_uri, preview_output_uri)
