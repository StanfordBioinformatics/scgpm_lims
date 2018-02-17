#!/usr/bin/env python

RUNS = [
    '141117_MONK_0387_AC4JCDACXX',
    '141126_PINKERTON_0343_BC4J1PACXX'
]

import glob
import os
from argparse import ArgumentParser
from components.connection import Connection

parser = ArgumentParser('Write LIMS data for a run to local disk for use in pipeline test mode')
parser.add_argument('--run_name')
parser.add_argument('--lims_url')
parser.add_argument('--lims_token')
args = parser.parse_args()

jsonfiles = glob.glob(os.path.join(os.path.dirname(__file__), 'testdata', '*.json'))
for jsonfile in jsonfiles:
    os.remove(jsonfile)

conn = Connection(lims_url=args.lims_url, lims_token=args.lims_token, testdata_update_mode=True, verbose=True)
for run in RUNS:
    conn.getallrunobjects(run=run)
