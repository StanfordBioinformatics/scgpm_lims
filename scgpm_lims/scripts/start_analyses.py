#!/usr/bin/env python3
# -*- coding: utf-8 -*-                                                                                
                                                                                                       
###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
###

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
            4) The sequencing instrument isn't a HiSeq 4000 (since those aren't supported yet in the pipeline).
"""
parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter,description=description)
parser.parse_args()

homedir = os.path.expanduser("~")
fout = open(os.path.join(homedir,"uhts_automated_analyses.txt"),"a")
conn = Connection()
runs = conn.getrunstoanalyze()
now = datetime.now()
for r in runs:
	if not runPaths.isCopyComplete(r):
		continue
	runinfo = conn.getruninfo(r)['run_info']
	pipeline_runs = runinfo['pipeline_runs']
	pipeline_run_ids = sorted(runinfo['pipeline_runs'],reverse=True)
	most_recent_run = ""
	most_recent_run_id = ""
	if pipeline_run_ids:
		most_recent_run_id = pipeline_run_ids[0]
		most_recent_run = pipeline_runs[most_recent_run_id]
		analysis_started = most_recent_run['started']
		if analysis_started:
			continue

	if not pipeline_runs:
		#create a pipeline run in UHTS
		conn.createpipelinerun(run=r)
		time.sleep(5) #there seems to be a delay in here, strangly, in getting the new pipeline run object to show
	#start the analysis pipeline
	cmd = "run_analysis.rb start_illumina_run --run {run} --force --verbose".format(run=r)
	if most_recent_run:
		cmd += " --rerun --pipeline-run-id {}".format(most_recent_run_id)
	if r.find("SPENSER") >= 0 or r.find("HOLMES") >= 0 or r.find("M04199") >= 0: #MiSeqs
		cmd += " --lanes 1"
	fout.write(str(now) + "  " + cmd + "\n")
	fout.flush()
	print(cmd)
	subprocess.Popen(cmd,shell=True,stderr=fout,stdout=fout)

