from stratus_endpoint.handler.base import TaskResult
from typing import List, Dict
from stratus_endpoint.handler.execution import Executable, ExecEndpoint
import xarray as xa
import abc
import glob
import MODIS_Aggregation.cloud_fraction_aggregate as modis
from  .laads_data_download import  sync
import os.path
import time


class XaOpsEndpoint(ExecEndpoint):
    """
        This class is used to implement the capabilities of the Endpoint.
    """

    @abc.abstractmethod
    def createExecutable(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs) -> Executable:
        """
            Factory method for Executable objects.
            Creates an Executable for each analytics operation

            Parameters:
            requestSpec (Dict):         Dict which defines the analytics operation
            inputs: (List[TaskResult]): Inputs from other operations in the workflow

            Returns:
            Executable: A Executable object which execute the operation.
            """
        return XaOpsExecutable(requestSpec, inputs, **kwargs)

    def init(self):
        """
            Used to startup analytic resources such as scheduler, worker nodes, database, etc.
            """
        return

    def shutdown(self, **kwargs):
        """
            Used to shut down and release resources that were alloacted in the init method.
            """
        return

    def capabilities(self, type: str, **kwargs) -> Dict:
        """
            Used to return metadata describing the capabilities of the Endpoint.
            The only required response is the definition the Endpoint Address (epa) for this Endpoint.
            The epa is used to route operation requests to the an Endpoint that can process them.

            Parameters:
                type (str):  Optionally allows the definition of various types of capabilty requests.

            Returns:
                  Dict:  metadata describing the capabilities of the Endpoint.
            """
        return dict(epas=["xop*"])


class XaOpsExecutable(Executable):
    """
        This class is used to implement a single operation.
    """

    def execute(self, **kwargs) -> TaskResult:
        """
            Executes the operation.
            Creates an Executable for each analytics operation
            The operation request is available as self.request.
            The operation inputs are available as self.inputs.

            Returns:
            TaskResult: The result of the operation.
            """
        print(f"Executing request {self.request}")

        # inputSpec = self.request.get('input', [])
        # M03_dir = inputSpec['path1']
        # M06_dir = inputSpec['path2']

        #Programmatically downloading the data from URL
        start = time.time()

        M03_source_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MYD03/2008/001/"
        M06_source_url = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/61/MYD06_L2/2008/001/"
        M03_localpath = "../new_data/MYD03"
        M06_localpath = "../new_data/MYD06_L2"

        token =  "F43033A6-B1DB-11EA-9C3C-E3E73909D347"

        if not os.path.exists(M03_localpath):
            os.makedirs(M03_localpath)

        if not os.path.exists(M06_localpath):
            os.makedirs(M06_localpath)

        sync(M03_source_url, M03_localpath, token)
        sync(M06_source_url, M06_localpath, token)
        
        print("\n\nCompleted download in " + str(time.time() - start) + " seconds")

        # M03_files = sorted(glob.glob(M03_dir + "MYD03.A2008*"))
        # M06_files = sorted(glob.glob(M06_dir + "MYD06_L2.A2008*"))
        M03_files = sorted(glob.glob(M03_localpath + "/MYD03.A2008*"))
        M06_files = sorted(glob.glob(M06_localpath + "/MYD06_L2.A2008*"))

        cf = self.operate(M03_files, M06_files)
        resultDataset = xa.DataArray(cf.tolist(), name='test')
        return TaskResult(kwargs, [resultDataset])

    def operate(self, M03_files, M06_files):
        """
            Convenience method defined for this particular operation
        """
        request = self.request['operation'][0]['name'].split(':')[1]

        start_aggr = time.time()

        if request == "cloudFraction":
            cf = modis.calculateCloudFraction(M03_files, M06_files)
            print("\n\nCompleted aggregation in " + str(time.time() - start_aggr) + " seconds")
        else:
            raise Exception(f"Unknown operation: '{request}'")
        return cf
