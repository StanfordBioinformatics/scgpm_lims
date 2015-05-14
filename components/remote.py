import json
import requests
import warnings
import os

warnings.filterwarnings('ignore', 'Unverified HTTPS request is being made. Adding certificate verification is strongly advised. See: https://urllib3.readthedocs.org/en/latest/security.html')

class RemoteDataManager:

    localorremote = 'remote'

    def __init__(self, apiversion=None, lims_url=None, lims_token=None, verify=False):

        if not apiversion:
            raise Exception('apiversion is required')
        self.apiversion = apiversion

        if (not lims_url) or not (lims_token):
            raise Exception("lims_url and lims_token are required. Current settings are lims_url=%s, lims_token=%s" % (lims_url, lims_token))
        self.token = lims_token
        self.urlprefix = self._geturlprefix(lims_url, apiversion)
        self.verify = verify

    def getsamplesheet(self, run, lane=None):
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

    def get_runinfo_by_library_name(self,library_name):
        url = self.urlprefix + "run_info_by_library_name"
        print(url)
        response = requests.get(
            url,
            params = {
                'token': self.token,
                'starts_with': library_name
            },
            verify = self.verify
        )
        self._checkstatus(response)
        return response.json()

    def getrunid(self, run):
        runinfo = requests.getruninfo(run)
        try:
            id = runinfo.get('id')
        except:
            return None
        return id

    def getlaneid(self, run, lane):
        runinfo = requests.getruninfo(run)
        try:
            id = runinfo.get('run_info').get('lanes').get(str(lane)).get('id')
        except:
            return None
        return id

    def showsolexarun(self, id):
        response = requests.get(
            self.urlprefix+'solexa_runs/%s' % id,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showsolexaflowcell(self, id):
        response = requests.get(
            self.urlprefix+'solexa_flow_cells/%s' % id,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showpipelinerun(self, id):
        response = requests.get(
            self.urlprefix+'solexa_pipeline_runs/%s' % id,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showlaneresult(id):
        response = requests.get(
            self.urlprefix+'solexa_lane_results/%s' % id,
            params = {
                'token': self.token
                },
            verify = self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def showmapperresult(self, id):
        response = requests.get(
            self.urlprefix+'mapper_results/%s' % id,
            params = {
                'token': self.token
                },
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def indexsolexaruns(self, run):
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
        response = requests.post(
            self.urlprefix+'mapper_results',
            params = {'token': self.token},
            data = json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatesolexarun(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'solexa_runs/%s' % id,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatesolexaflowcell(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'solexa_flow_cells/%s' % id,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatepipelinerun(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'solexa_pipeline_runs/%s' % id,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatelaneresult(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'solexa_lane_results/%s' % id,
            params = {
                'token': self.token
                },
            data=json.dumps(paramdict),
            headers = {'content-type': 'application/json'},
            verify=self.verify,
            )
        self._checkstatus(response)
        return response.json()

    def updatemapperresult(self, id, paramdict):
        response = requests.patch(
            self.urlprefix+'mapper_results/%s' % id,
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
        
        return os.path.join(rooturl,"api",apiversion) + "/"

    def _checkstatus(self, response):

        if not response.ok:
            raise Exception("%s response. %s. %s" % (response.status_code, response.reason, response.url))

