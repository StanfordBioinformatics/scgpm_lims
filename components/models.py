import re

# These are convenience classes. You can work directly with the Connection class
# and the data objects that it returns, but any methods for working with those
# data objects live here.

class SolexaRun:

    STATUS_SEQUENCING = 'sequencing'
    STATUS_SEQUENCING_DONE = 'sequencing_done'
    STATUS_SEQUENCING_FAILED = 'sequencing_failed'
    STATUS_SEQUENCING_EXCEPTION = 'sequencing_exception'
    STATUS_PREPROCESSING = 'preprocessing'

class SolexaFlowCell:

    STATUS_INCOMPLETE = 'incomplete'
    STATUS_ASSIGNED = 'assigned'
    STATUS_CLUSTERING = 'clustering'
    STATUS_SEQUENCING = 'sequencing'
    STATUS_ANALYZING = 'analyzing'
    STATUS_REVIEWING = 'reviewing'
    STATUS_DONE = 'done'
    STATUS_CANCELLED = 'cancelled'

class RunInfo:

    def __init__(self, conn, run):
        self.conn = conn
        self.run = run
        self._refresh()

    def _refresh(self):
        obj = self.conn.getruninfo(run=self.run)
        self.data = obj['run_info']
        self.solexarunid = obj['id']

    def get_solexa_run_status(self):
        return self.data['sequencing_run_status']

    def get_flow_cell_status(self):
        return self.data['flow_cell_status']

    def get_solexa_run_name(self):
        return self.data['run_name']

    def get_solexa_run_id(self):
        return self.solexarunid

    def get_sequencing_instrument(self):
        return self.data['sequencing_instrument']

    def get_data_volume(self):
        return self.data['data_volume']

    def get_sequencer_software(self):
        return self.data['seq_software']

    def is_paired_end(self):
        return self.data['paired_end']

    def has_index_read(self):
        return self.data['index_read']

    def get_read1_cycles(self):
        return self.data.get('read1_cycles')

    def get_read2_cycles(self):
        return self.data.get('read2_cycles')

    def get_solexa_flow_cell_id(self):
        return self.data['flow_cell_id']
      
    def get_lane(self,lane):
        lane = str(lane)
        return self.Lane(self.data['lanes'][lane])

    def get_pipeline_run(self, lane=None, status='done'):
        VALID_STATA = ['done', 'inprogress', 'new']
        if status not in VALID_STATA:
            raise Exception('Invalid pipeline run status "%s" was requested.'
                            % (status, VALID_STATA))

        done = {}
        new = {}
        inprogress = {}

        for run_id, run in self.data['pipeline_runs'].iteritems():
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
    
    def has_status_sequencing_failed(self):
        return self.get_solexa_run_status() == SolexaRun.STATUS_SEQUENCING_FAILED

    def is_analysis_done(self):
        return self.data['analysis_done']

    def set_flags_for_sequencing_failed(self):
        solexarunupdate = {
            'sequencer_done': True,
            'analysis_done': True,
            'dnanexus_done': False,
            'notification_done': True,
            'archiving_done': True
        }
        self.conn.updatesolexarun(self.get_solexa_run_id(), solexarunupdate)

        solexaflowcellupdate = {
            'flow_cell_status': SolexaFlowCell.STATUS_DONE
        }
        self.conn.updatesolexaflowcell(self.get_solexa_flow_cell_id(), solexaflowcellupdate)
        self._refresh()

    def set_flags_for_sequencing_finished_analysis_started(self):
        solexarunupdate = {
            'sequencer_done': True
        }
        self.conn.updatesolexarun(self.get_solexa_run_id(), solexarunupdate)

        solexaflowcellupdate = {
            'flow_cell_status': SolexaFlowCell.STATUS_ANALYZING
        }
        self.conn.updatesolexaflowcell(self.get_solexa_flow_cell_id(), solexaflowcellupdate)
        self._refresh()

