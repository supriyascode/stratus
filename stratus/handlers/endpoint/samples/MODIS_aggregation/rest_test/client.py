from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import time
from stratus.app.core import StratusCore
import os
import xarray as xa
import pandas as pd
import pdb
import numpy as np
from datetime import date, datetime
from dateutil.rrule import rrule, DAILY, MONTHLY


if __name__ == "__main__":
    start = time.time()

    #  Startup a Stratus zmq client and connect to a server on localhost

    settings = dict(stratus=dict(type="rest", client_address="127.0.0.1", host= "127.0.0.1", port = "5000", api = "core"))

    stratus = StratusCore(settings)
    client = stratus.getClient()

    print("\n\n in rest client")

    df_datapath = pd.read_csv('data_path.csv',  header=0, delim_whitespace=True)
    df_output_path = pd.read_csv('data_path.csv',  header=3, delim_whitespace=True)
    df_varlist = pd.read_csv('input_file.csv',  header=0, delim_whitespace=True)
    df_jvarlist = pd.read_csv('input_Jhist.csv',  header=0, delim_whitespace=True)
    str_start_date='2008/01/01'
    str_end_date='2008/01/01'               


    requestSpec = dict(
        input=dict(data_path_file=df_datapath.to_json(), 
                   output_path = df_output_path.to_json(),
                   start_date=str_start_date, 
                   end_date=str_end_date, 
                   poly='[-90,90,-180,180]', 
                   grid='[1,1]', 
                   spl_num='[5]',
                   sts_switch='1, 1, 1, 1, 1, 1, 1',
                   varlist=df_varlist.to_json(),
                   jvarlist=df_jvarlist.to_json()
                   ),
        operation=[dict(name="xop:modisAggr")]
    )
 

    # Submit the request to the server and wait for the result
    task: TaskHandle = client.request(requestSpec, input)
    result: Optional[TaskResult] = task.getResult(block=True)

    # Display the result

    print("\n\nCompleted computation in " + str(time.time() - start) + " seconds")

    result_file = result.data[0] 


    #ouput dir and file name
    output_dir = np.array(df_output_path)[0,0]
    output_prefix = np.array(df_output_path)[0,1]
    
    if os.path.exists(output_dir) is False:
        os.makedir(output_dir)

    start_date = np.fromstring(str_start_date, dtype=np.int, sep='/' )
    end_date   = np.fromstring(str_end_date, dtype=np.int, sep='/' )
    start = date(start_date[0], start_date[1], start_date[2])
    until = date(end_date[0], end_date[1], end_date[2])

    for dt in rrule(DAILY, interval=1, dtstart=start, until=until):
        year  = np.int(dt.strftime("%Y"))
        month = np.int(dt.strftime("%m"))

    l3name  = output_prefix + '.A{:04d}{:02d}'.format(year,month)
    subname = '_baseline_monthly_v8_client.h5'
    subname2 = '_baseline_monthly_v8_client.nc'

    # result_file.to_netcdf(os.path.join(output_dir,'baseline_monthly_v8_client.h5'), mode='w', format='NETCDF4')
    result_file.to_netcdf(os.path.join(output_dir,l3name+subname), mode='w', format='NETCDF4')
    ## use hdf5 format and not netcdf4 -- Check a different API with different format
    


    print("Result saved at " + output_dir)
