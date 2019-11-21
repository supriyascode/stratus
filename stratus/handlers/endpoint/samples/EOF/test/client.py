from stratus_endpoint.handler.base import TaskHandle, TaskResult
from typing import Sequence, List, Dict, Mapping, Optional, Any
import time, xarray as xa
from stratus.app.core import StratusCore
from eofs.examples import example_data_path
import matplotlib.pyplot as plt
import numpy as np

import cartopy.crs as ccrs
import cartopy.feature as cfeature

USE_OPENDAP = True

def displayOutput(dset):
    clevs = np.linspace(-1, 1, 11)
    fig = plt.figure(figsize=(15, 15))
    ax = plt.axes(projection=ccrs.PlateCarree(central_longitude=190))
    fill = dset.to_array()[0][0].plot.contourf(ax=ax, levels=clevs, cmap=plt.cm.RdBu_r,
                                               add_colorbar=False, transform=ccrs.PlateCarree())
    ax.add_feature(cfeature.LAND, facecolor='w', edgecolor='k')
    cb = plt.colorbar(fill, orientation='horizontal')
    cb.set_label('correlation coefficient', fontsize=12)
    ax.set_title('EOF1 expressed as correlation', fontsize=16)
    plt.xlim(-100, 100)
    plt.ylim(-100, 100)
    plt.show()

if __name__ == "__main__":
    start = time.time()

#  Startup a Stratus zmq client and connect to a server on localhost

    settings = dict(stratus=dict(type="zeromq", client_address="127.0.0.1", request_port="4566", response_port="4567"))
    stratus = StratusCore(settings)
    client = stratus.getClient()

# Define an analytics request (time average of merra2 surface temperature) directed to the 'xop' endpoint

    filename = example_data_path('sst_ndjfm_anom.nc')
    requestSpec = dict(
        input=dict(filename=filename, name=f"sst"),
        operation=[dict(name="xop:correlation", axis="time")]
    )

# Submit the request to the server and wait for the result

    task: TaskHandle = client.request(requestSpec)
    result: Optional[TaskResult] = task.getResult(block=True)

# Print result metadata and save the result to disk as a netcdf file

    print("\n\nCompleted computation in " + str(time.time() - start) + " seconds")
    for ind, dset in enumerate(result.data):
        print(f"Received result dataset containing variables: ")
        for name, var in dset.data_vars.items():
            print( f"\t {name}:  dims = {var.dims}, shape = {var.shape}")
        rpath = f"/tmp/endpoint-sample-result-{ind}.nc"
        print( f"Saving result to {rpath}\n\n")
        dset.to_netcdf( rpath )
        displayOutput(dset)




