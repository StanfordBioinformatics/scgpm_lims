import json
import requests

class RemoteDataManager:

    localorremote = 'remote'

    def __init__(self, apiversion=None, lims_url=None, lims_token=None, read_lims=True, write_lims=True, verify=False):

        self.read_lims = read_lims
        self.write_lims = write_lims

        if not apiversion:
            raise Exception('apiversion is required')
        self.apiversion = apiversion

        if (not lims_url) or not (lims_token):
            raise Exception("lims_url and lims_token are required. Current settings are lims_url=%s, lims_token=%s" % (lims_url, lims_token))
        self.token = lims_token
        self.urlprefix = self._geturlprefix(lims_url, apiversion)
        self.verify = verify

    def getsamplesheet(self, run, lane=None):
        if not self.read_lims:
            return None

        params = {
            'token': self.token,
            'run': run
            }
        if lane is not None:
            params['lane'] = str(lane)

        response = requests.get(
            self.urlprefix+'samplesheets', 
            params=params,
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.text

    def getruninfo(self, run):
        if not self.read_lims:
            return None

        response = requests.get(
            self.urlprefix+'run_info',
            params = {
                'token': self.token,
                'run': run
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def getrunid(self, run):
        if not self.read_lims:
            return None

        runinfo = self.getruninfo(run)
        try:
            idd = runinfo.get('id')
        except:
            return None
        return idd

    def getlaneid(self, run, lane):
        if not self.read_lims:
            return None

        runinfo = self.getruninfo(run)
        try:
            idd = runinfo.get('run_info').get('lanes').get(str(lane)).get('id')
        except:
            return None
        return idd

    def showsolexarun(self, idd):
        if not self.read_lims:
            return None

        response = requests.get(
            self.urlprefix+'solexa_runs/%s' % idd,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showpipelinerun(self, idd):
        if not self.read_lims:
            return None

        response = requests.get(
            self.urlprefix+'solexa_pipeline_runs/%s' % idd,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showlaneresult(idd):
        if not self.read_lims:
            return None

        response = requests.get(
            self.urlprefix+'solexa_lane_results/%s' % idd,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showmapperresult(self, idd):
        if not self.read_lims:
            return None

        response = requests.get(
            self.urlprefix+'mapper_results/%s' % idd,
            params = {
                'token': self.token
                },
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def indexsolexaruns(self, run):
        if not self.read_lims:
            return {}

        response = requests.get(
            self.urlprefix+'solexa_runs',
            params = {
                'token': self.token,
                'run': run
                },
            verify=self.verify,
            )

        self._checkstatus(response)
        return self._listtodict(response.json())

    def indexpipelineruns(self, run):
        if not self.read_lims:
            return {}

        response = requests.get(
            self.urlprefix+'solexa_pipeline_runs',
            params = {
                'token': self.token,
                'run': run
                },
            verify=self.verify,
            )
        self._checkstatus(response)
        return self._listtodict(response.json())

    def indexlaneresults(self, run, lane=None, barcode=None, readnumber=None):
        if not self.read_lims:
            return {}

        params = {'run': run,
                  'token': self.token}
        if lane is not None:
            params.update({'lane': lane})
            if barcode is not None:
                params.update({'barcode': barcode})
                if readnumber is not None:
                    params.update({'read_number': readnumber})
        response = requests.get(
            self.urlprefix+'solexa_lane_results',
            params = params,
            verify=self.verify,
            )

        self._checkstatus(response)
        return self._listtodict(response.json())

    def indexmapperresults(self, run):
        if not self.read_lims:
            return {}

        response = requests.get(
            self.urlprefix+'mapper_results',
            params = {
                'token': self.token,
                'run': run
                },
            verify=self.verify,
            )
        self._checkstatus(response)

        return self._listtodict(response.json())

    def createpipelinerun(self, run, lane, paramdict = None):
        if not self.write_lims:
            return None

        if paramdict:
            data = json.dumps(paramdict)
        else:
            data = None

        response = requests.post(
            self.urlprefix+'solexa_pipeline_runs',
            params = {
                'run': run,
                'token': self.token
                },
            data = data,
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def createlaneresult(self, paramdict, run=None, lane=None):
        if not self.write_lims:
            return None

        params = {'token': self.token}
        if run is not None:
            params.update({'run': run})
            if lane is not None:
                params.update({'lane': lane})
        response = requests.post(
            self.urlprefix+'solexa_lane_results',
            params = params,
            data = json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def createmapperresult(self, paramdict):
        if not self.write_lims:
            return None
        
        response = requests.post(
            self.urlprefix+'mapper_results',
            params = {'token': self.token},
            data = json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatesolexarun(self, idd, paramdict):
        if not self.write_lims:
            return None

        response = requests.patch(
            self.urlprefix+'solexa_runs/%s' % idd,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatepipelinerun(self, idd, paramdict):
        if not self.write_lims:
            return None

        response = requests.patch(
            self.urlprefix+'solexa_pipeline_runs/%s' % idd,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatelaneresult(self, idd, paramdict):
        if not self.write_lims:
            return None

        response = requests.patch(
            self.urlprefix+'solexa_lane_results/%s' % idd,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatemapperresult(self, idd, paramdict):
        if not self.write_lims:
            return None

        response = requests.patch(
            self.urlprefix+'mapper_results/%s' % idd,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def deletelaneresults(self, run, lane):

        # for each lane
        #   for each lane_result
        #     Select pipeline_runs where id = lane_result.solexa_pipeline_run_id
        #     for each, active = false
        #     select each mapper_result where dataset_id = lane_result.id
        #     for each, active = false
        #     lane_result.active = false
        #                                                                                                                                                                                                   
        response = requests.post(
            self.urlprefix+'delete_lane_results',
            params = {
                'run': run,
                'lane': lane,
                'token': self.token
                },
            verify=self.verify,
            )
        self._checkstatus(response)

    def testconnection(self):
        if not self.read_lims:
            return

        response = requests.get(
            self.urlprefix+'ok',
            params = {
                'token': self.token
                },
            verify=self.verify,
            )
        self._checkstatus(response)
        return

    def _listtodict(self, resultslist):
        resultsdict = {}
        for result in resultslist:
            resultsdict[str(result.get('id'))] = result
        return resultsdict

    def _geturlprefix(self, rooturl, apiversion):
        
        return '%s/api/%s/' % (rooturl, apiversion)

    def _checkstatus(self, response):

        if not response.ok:
            raise Exception("%s response. %s. %s" % (response.status_code, response.reason, response.url))
