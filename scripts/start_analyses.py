#!/usr/bin/env python

import time
import sys 
import os
import json
import argparse
from datetime import datetime
import subprocess

from scgpm_lims import Connection
from gbsc_utils.SequencingRuns import runPaths

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
fout = open(os.path.join(homedir,"uhts_automated_analyses.txt"),"a")
conn = Connection()
runs = conn.getrunstoanalyze()
now = datetime.now()
print(runs)
#runs = ['151104_BRISCOE_0266_BC7M9AACXX', '151104_BRISCOE_0265_AC7KVTACXX', '151103_MONK_0450_AC7MCWACXX', '151110_SPENSER_0226_000000000-AG4GH']
for r in runs:
	#create a pipeline run in UHTS
	if not runPaths.isCopyComplete(r):
		continue
	conn.createpipelinerun(run=r)
	time.sleep(5) #there seems to be a delay in here, strangly, in getting the new pipeline run object to show
	#start the analysis pipeline
	cmd = "run_analysis.rb start_illumina_run --run {run} --force --verbose".format(run=r)
	if "SPENSER" or "HOLMES" in r: #MiSeqs
		cmd += " --lanes 1"
	fout.write(str(now) + "  " + cmd + "\n")
	fout.flush()
	#subprocess.Popen(cmd,shell=True,stderr=fout,stdout=fout)
	fout.flush()

