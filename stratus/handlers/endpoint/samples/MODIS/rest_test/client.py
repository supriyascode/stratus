from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import time
from stratus.app.core import StratusCore
import MODIS_Aggregation.cloud_fraction_aggregate as modis
import os
HERE: str = os.path.dirname(os.path.abspath(__file__))
SETTINGS_FILE: str = os.path.join( HERE, "server_settings.ini" )

if __name__ == "__main__":
    start = time.time()

    #  Startup a Stratus zmq client and connect to a server on localhost

    stratus = StratusCore(SETTINGS_FILE)
    client = stratus.getClient()

    M03_dir = "/Users/lakshmipriyanka/Project/MODIS_Aggregation/resources/data/input_data_sample/MYD03/"
    M06_dir = "/Users/lakshmipriyanka/Project/MODIS_Aggregation/resources/data/input_data_sample/MYD06/"
    requestSpec = dict(
        input=dict(path1=M03_dir, path2=M06_dir, name1=["Latitude", "Longitude"], name2=["Cloud_Mask_1km"]),
        operation=[dict(name="xop:cloudFraction")]
    )

    # Submit the request to the server and wait for the result

    task: TaskHandle = client.request(requestSpec)
    result: Optional[TaskResult] = task.getResult(block=True)

    # Display the result

    print("\n\nCompleted computation in " + str(time.time() - start) + " seconds")
    modis.displayOutput(result.getDataset().to_array()[0])
