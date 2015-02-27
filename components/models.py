import re
class RunInfo:

    #TODO fix this enum
    RUN_STATUS_COPY_STARTED = 0
    RUN_SEQUENCING_FAILED = 0

    def __init__(self, conn, run):
        obj = conn.getruninfo(run=run)
        self.ri = obj['run_info'] #self.ri = run info object
        self.runId =obj['id'] 

    def getRunName(self):
        return self.ri['run_name']
      
    def getLane(self,lane):
        lane = str(lane)
        return self.Lane(self.ri['lanes'][lane])

    def setRunStatus(self, status):
        pass #TODO

    def getRunStatus(self, status):
        pass #TODO

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

    class LaneInfo:
        emailReg = re.compile('\w{3,20}@\w{3,20}\.\w{3}')
        def __init__(self,laninfo):
            """
            Args: laneinfo - A dict that is one of the values of the lanes in self.ri['lanes'].
            """
            self.li = laneinfo
      
        def __getitem__(self,key):
            return self.li[key] 

        def getDnaLibraryId(self):
            return self['dna_library_id']

        def getBarcodeSize1(self):
            return self['barcode_size']

        def getLab(self):
            return self['lab']
 
        def getMappingRequests(self):
            return self['mapping_requests']

        def getQueue(self):
            return self['queue']
 
        def getSubmitter(self):
            return self['submitter']
      
        def getBarcodePosition(self):
            return self['barcode_position']

        def getSequencingRequest(self):
            return self['sequencing_request']

        def getNotify(self):
            return self['notify']

        def getSampleName(self):
            return self['sample_name']

        def getOwner(self):
            return self['owner']
   
        def getBarcodeSize2(self):
            return self['barcode_size2']
        
        def isMultiplexed(self):
            return self['multiplexed']
 
            self.notify_emails = [x['email'].strip() for x in self.li['notify']]
            notify_comments = None
            try:
                notify_comments = self.li['notify_comments']
            except KeyError:
                pass
            if notify_comments:
                potentialEmails = re.split(r'[ ,;]',notify_comments)
                potentialEmails = [x.strip() for x in potentialEmails]
                potentialEmails = emaiReg.findall(potentialEmails)
                if potentialEmails:
                    for i in potentialEmails:
                        self.notify_emails.append(i)

        def getLaneId(self):
            return self['id']

        def getSubmitterEmail(self):
            return self['submitter_email']

        def getBarcodes(self):
            return self['barcodes']
