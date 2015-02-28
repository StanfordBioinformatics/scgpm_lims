import re
class RunInfo:

    def __init__(self, conn, run):
        obj = conn.getruninfo(run=run)
        self.data = obj['run_info']

    def getRunName(self):
        return self.data['run_name']
      
    def getLane(self,lane):
        lane = str(lane)
        return self.Lane(self.data['lanes'][lane])

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

class SolexaRun:

    STATUS_SEQUENCING = 'sequencing'
    STATUS_SEQUENCING_DONE = 'sequencing_done'
    STATUS_SEQUENCING_FAILED = 'sequencing_failed'
    STATUS_SEQUENCING_EXCEPTION = 'sequencing_exception'
    STATUS_PREPROCESSING = 'preprocessing'

    def __init__(self, conn, run_id=None, run_name=None):
        if run_id is not None:
            if run_name is not None:
                raise Exception("Specify run_id or run_name but not both. run_id=%s, run_name=%s" % (run_id, run_name))
            run = conn.showsolexarun(run_id)
        elif run_name is not None:
            run = conn.indexsolexaruns(run_name)
            if len(run) > 1:
                raise Exception("More than 1 run found with run_name=%s" % run_name)
            else:
                raise Exception("No run found with run_name=%s" % run_name)
        else:
            raise Exception("Either run_id or run_name is required")

        for (id, data) in run.iter_items():
            # There is only 1. This isn't a real loop.
            self.id = id
            self.data = data

    def getStatus(self):
        return self.data['sequencing_run_status']

    def setStatus(self, status, save=True):
        self.data['sequencing_run_status'] = status
        if save:
            self.save()

    def save(self):
        conn.updatesolexarun(self.id, self.data)

        
