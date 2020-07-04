from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import time
from stratus.app.core import StratusCore
import MODIS_Aggregation.cloud_fraction_aggregate as modis

if __name__ == "__main__":
    start = time.time()

    #  Startup a Stratus zmq client and connect to a server on localhost

    settings = dict(stratus=dict(type="rest", client_address="127.0.0.1", host= "127.0.0.1", port = "5000", api = "core"))

    stratus = StratusCore(settings)
    client = stratus.getClient()

    # M03_dir = "/Users/lakshmipriyanka/Project/MODIS_Aggregation/resources/data/input_data_sample/MYD03/"
    # M06_dir = "/Users/lakshmipriyanka/Project/MODIS_Aggregation/resources/data/input_data_sample/MYD06/"
    # requestSpec = dict(
    #     input=dict(path1=M03_dir, path2=M06_dir, name1=["Latitude", "Longitude"], name2=["Cloud_Mask_1km"]),
    #     operation=[dict(name="xop:cloudFraction")]
    # )

    requestSpec = dict(
        input=dict(area=["Latitude", "Longitude"], cloud_mask=["Cloud_Mask_1km"]),
        operation=[dict(name="xop:cloudFraction")]
    )

    # Submit the request to the server and wait for the result

    task: TaskHandle = client.request(requestSpec)
    result: Optional[TaskResult] = task.getResult(block=True)

    # Display the result

    print("\n\nCompleted computation in " + str(time.time() - start) + " seconds")
    modis.displayOutput(result.getDataset().to_array()[0])
