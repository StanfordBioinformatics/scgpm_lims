#!/usr/local/opt/python3/bin/python3.6
# -*- coding: utf-8 -*-                                                                                
                                                                                                       
###                                                                                                    
# © 2018 The Board of Trustees of the Leland Stanford Junior University                              
# Nathaniel Watson                                                                                      
# nathankw@stanford.edu                                                                                 
# 2018-01-09
###

import os
import argparse

from scgpm_lims import Connection

description = "Given a file containing rows of flow cell IDs, retrieves the corresponding run names."
parser = argparse.ArgumentParser(description=description,formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument("-i","--infile",required=True,help="The input file containing a single column with the flowcell ID, one per row. For example, if the run name is 170802_COOPER_0128_AHK23YBBXX, you'd enter AHK23YBBXX as the flowcell ID, or just the part that UHTS specifically tracks, which is HK23Y (the first five characters that follow the first character).")
parser.add_argument("-o","--outfile",required=True,help="The output file, which is the same as the input file but with an extra column being the associated UHTS run name (tab-delimited).")

args = parser.parse_args()

infile = args.infile
if not os.path.exists(infile):
	parser.error("Input file '{}' does not exist.".format(infile))
outfile = args.outfile

uhts = Connection()

fh = open(infile)
fout = open(outfile,'w')
for line in fh:
	line = line.strip()
	if not line:
		continue
	if len(line) > 5: #if == 5, already the shortened version that the SolexaFlowCell.flowcell_id attribute stores.
		line = line[1:6]
	run_name = uhts.get_runname_from_flowcell_id(line)	
	fout.write(line + "\t" + run_name + "\n")
fh.close()
fout.close()

