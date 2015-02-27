class RunInfo:

    # Allowed values for SolexaRun.sequencingRunStatus
    SEQUENCING_RUN_STATUS_PREPROCESSING = 'preprocessing'
    SEQUENCING_RUN_STATUS_SEQUENCING = 'sequencing'
    SEQUENCING_RUN_STATUS_DONE = 'sequencing_done'
    SEQUENCING_RUN_STATUS_FAILED = 'sequencing_failed'
    SEQUENCING_RUN_STATUS_EXCEPTION = 'sequencing_exception'

    def __init__(self, conn, run):
        self.obj = conn.getruninfo(run=run)

    def get_run_name(self):
        return self.obj['run_info']['run_name']
      
    def get_run_status(self):
        return self.obj['run_info']['sequencing_run_status']

    def getpipelinerunid(self, run, lane=None, status='done'):
        VALID_STATA = ['done', 'inprogress', 'new']
        if status not in VALID_STATA:
            raise Exception('Invalid pipeline run status "%s" was requested.'
                            % (status, VALID_STATA))
        run_info = self.getruninfo(run)

        done = {}
        new = {}
        inprogress = {}

        for run_id, run in run_info['run_info']['pipeline_runs'].iteritems():
            if run['finished'] == True:
                done[run_id] = run
            elif run['started'] == False and run['finished'] == False:
                new[run_id] = run
            elif run['started'] == True and run['finished'] == False:
                inprogress[run_id] = run

        def _getlatest(pipeline_runs, status):
            if len(pipeline_runs.keys()) == 0:
                raise Exception("No pipeline runs found with status %s" % status)
            run_id = max(pipeline_runs.keys())
            run = pipeline_runs[run_id]
            return (run_id, run)

        if status == 'done':
            pipeline_runs = done
        elif status == 'new':
            pipeline_runs = new
        elif status == 'inprogress':
            pipeline_runs = inprogress

        return _getlatest(pipeline_runs, status)

class SolexaRun:
    def __init__(self, conn, run_id=None): 
        self.obj = conn.showrun(run=run)
        

#    class Lane:
#        emailReg = re.compile('\w{3,20}@\w{3,20}\.\w{3}')
#        def __init__(self,laninfo):
#            self.laneinfo = laneinfo
#            self.notify_emails = [x['email'].strip() for x in self.laneinfo['notify']]
#            notify_comments = None
#            try:
#                notify_comments = self.laneinfo['notify_comments']
#            except KeyError:
#                pass
#            if notify_comments:
#                potentialEmails = re.split(r'[ ,;]',notify_comments)
#                potentialEmails = [x.strip() for x in potentialEmails]
#                potentialEmails = emaiReg.findall(potentialEmails)
#                if potentialEmails:
#                    for i in potentialEmails:
#                        self.notify_emails.append(i)
                        
#    def __init__(self,runinfo):
#        """
#        Args : runinfo - dict. of the kind returned from Connection.getruninfo().
#        """
#        ri = runinfo['run_info']
#        self.paired_end = ri['paired_end'] #bool
#        self.read1_cycles = ri['read1_cycles'] #int
#        self.read2_cycles = ri['read2_cycles'] #int
#        self.index1_cycles = ri['index1_cycles'] #int
#        self.index2_cycles = ri['index2_cycles'] #int
#        self.control_lane = ri['control_lane'] #int - lane number of the control if there is a control. Not sure what it returns if no control
#        self.seq_software = ri['seq_software'] #str (i.e. 'hcs_2_0_5')
#        self.flow_cell = ri['flow_cell'] #str
#        self.platform = ri['platform_name'] #str
#        self.run_name = ri['run_name'] #str i.e. '140415_BRISCOE_0154_BC42Y8ACXX'
#        self.lanes = {}
#        for i in ri['lanes']:
#            self.lanes[int(i)] = Lane(i)

