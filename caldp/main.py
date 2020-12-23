"""This module integrates basic calibration processing and preview generation,
initially for the purpose of supporting testing.
"""

from caldp import process
from caldp import create_previews
from caldp import messages


def main(ipppssoot, input_uri, output_uri):
    process.process(ipppssoot, input_uri, output_uri)
    create_previews.main(input_uri, output_uri, ipppssoot)
    messages.main(input_uri, output_uri, ipppssoot)
