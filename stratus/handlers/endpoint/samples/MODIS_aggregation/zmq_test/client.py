from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import time
from stratus.app.core import StratusCore
import os


if __name__ == "__main__":
    start = time.time()

    #  Startup a Stratus zmq client and connect to a server on localhost

    settings = dict(stratus=dict(type="zeromq", client_address="127.0.0.1", request_port="4566", response_port="4567"))
    stratus = StratusCore(settings)
    client = stratus.getClient()

    #M03_dir = "/Users/lakshmipriyanka/Project/MODIS_Aggregation/resources/data/input_data_sample/MYD03/"
    #M06_dir = "/Users/lakshmipriyanka/Project/MODIS_Aggregation/resources/data/input_data_sample/MYD06/"

    # requestSpec = dict(
    #     input=dict(path1=M03_dir, path2=M06_dir, name1=["Latitude", "Longitude"], name2=["Cloud_Mask_1km"]),
    #     operation=[dict(name="xop:cloudFraction")]
    # )

    #arg_string = "data_path.csv 2008/01/01 2008/01/01 [-90,90,-180,180] [1,1] [5] 1 1 1 1 1 1 1 input_file.csv input_Jhist.csv"

    print("\n\n in client")

    requestSpec = dict(
        input=dict(data_path_file='data_path.csv', 
                   start_date='2008/01/01', 
                   end_date='2008/01/01', 
                   poly='[-90,90,-180,180]', 
                   grid='[1,1]', 
                   spl_num='[5]',
                   sts_switch='1, 1, 1, 1, 1, 1, 1',
                   varlist='input_file.csv',
                   jvarlist='input_Jhist.csv'
                   ),
        operation=[dict(name="xop:modisAggr")]
    )
 

    # Submit the request to the server and wait for the result

    task: TaskHandle = client.request(requestSpec)
    result: Optional[TaskResult] = task.getResult(block=True)

    # Display the result

    print("\n\nCompleted computation in " + str(time.time() - start) + " seconds")

    result_file = result.data[0] 
    
    output_dir = 'client_result'
    if os.path.exists(output_dir) is False:
        os.makedir(output_dir)
    
    result_file.to_netcdf(os.path.join(output_dir,'tmp.hdf5'), mode='w', format='NETCDF4')
    
    print("Result saved at " + output_dir)
