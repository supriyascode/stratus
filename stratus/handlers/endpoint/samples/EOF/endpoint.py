from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional, Callable
from stratus_endpoint.util.config import StratusLogger
from stratus_endpoint.handler.execution import Executable, ExecEndpoint
import xarray as xa
import abc
import numpy as np
from eofs.xarray import Eof


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
        inputSpec = self.request.get('input', [])
        dset: xa.Dataset = xa.open_dataset(inputSpec['filename'])
        vid = inputSpec['name']
        variable: xa.DataArray = dset[vid]
        result_arrays = self.operate(vid, variable)
        return TaskResult(kwargs, [result_arrays])

    def operate(self, vid: str, variable: xa.DataArray) -> Dict[str, xa.DataArray]:
        """
            Convenience method defined for this particular operation
        """
        opSpecs = self.request['operation']
        result_arrays: Dict[str, xa.DataArray] = {}
        for opSpec in opSpecs:
            opId = opSpec['name'].split(':')[1]
            opAxis = opSpec['axis']
            coslat = np.cos(np.deg2rad(variable.coords['latitude'].values))
            wgts = np.sqrt(coslat)[..., np.newaxis]
            solver = Eof(variable, weights=wgts)
            if opId == "correlation":
                result_arrays = solver.eofsAsCorrelation(neofs=1)
            elif opId == "covariance":
                result_arrays = solver.eofsAsCovariance(neofs=1)
            else:
                raise Exception(f"Unknown operation for EOF: '{opId}'")
        return result_arrays
