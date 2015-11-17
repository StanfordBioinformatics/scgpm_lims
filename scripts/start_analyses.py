#!/usr/bin/env python

import sys 
import os
import json
import argparse
from datetime import datetime
import subprocess

from scgpm_lims import Connection

try:
  token = os.environ["UHTS_LIMS_TOKEN"]
except KeyError:
  token = None
try:
  url = os.environ["UHTS_LIMS_URL"]
except KeyError:
  url = None

description = """
        Fetches the runs names from UHTS that need to have analyses started, and starts the analysis for each.
        Runs with the following criteria are returned:
            1) sequencing_run_status = sequencing_done
            2) analysis_done = false 
            3) There aren't any pipeline_runs
            4) The sequencing instrument isn't a HiSeq 4000 (since those aren't supported yet in the pipeline).
"""
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,description=description)

homedir = os.path.expanduser("~")
fout = open(os.path.join(homedir,"uhts_automated_analyses.txt"),"wa")
conn = Connection()
runs = conn.getrunstoanalyze()
now = datetime.now()
print(runs)
for r in runs:
	cmd = "run_analysis.rb start_illumina_run --run {run} --force --verbose".format(run=r)
	fout.write(str(now) + "  " + cmd + "\n")
	subprocess.Popen(cmd,shell=True,stderr=fout,stdout=fout)

