#!/usr/bin/env python

import sys
from scgpm_lims import Connection
import json
import argparse

parser = argparse.ArgumentParser( description='Create the sample sheet in the Stanford LIMS system')

parser.add_argument('-r', '--run_name', help='Name of the run.', required=True)
parser.add_argument('-t', '--lims_token', help='LIMS access token', required=True)
parser.add_argument('-u', '--lims_url', help='LIMS url', required=True)
parser.add_argument('-l','--lane',type=int,help="The number of the lane sequenced on the flowcell.")

args = parser.parse_args()
conn = Connection(lims_url=args.lims_url, lims_token=args.lims_token, verbose=False)

fn = args.run_name
if args.lane:
	fn += "_L" + str(args.lane)
fn += '_samplesheet.csv'
conn.getsamplesheet(run=args.run_name,lane=args.lane,filename=fn)
print fn
