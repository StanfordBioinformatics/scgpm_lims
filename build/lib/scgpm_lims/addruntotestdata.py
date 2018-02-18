#!/usr/bin/env python

from argparse import ArgumentParser
from scgpm_lims.components.connection import Connection

parser = ArgumentParser('Write LIMS data for a run to local disk for use in pipeline test mode')
parser.add_argument('--run_name')
parser.add_argument('--lims_url')
parser.add_argument('--lims_token')
args = parser.parse_args()

conn = Connection(lims_url=args.lims_url, lims_token=args.lims_token, testdata_update_mode=True, verbose=True)
conn.getallrunobjects(run=args.run_name)
