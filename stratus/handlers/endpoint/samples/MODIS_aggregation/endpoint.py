from stratus_endpoint.handler.base import TaskResult
from typing import List, Dict
from stratus_endpoint.handler.execution import Executable, ExecEndpoint
import xarray as xa
import abc
import source.baseline.MODIS_Aggregation_Serial as series
import numpy as np
from datetime import date, datetime
from dateutil.rrule import rrule, DAILY, MONTHLY
import h5py
import timeit
import pandas as pd
from netCDF4 import Dataset
import pdb
import json
import calendar
from dask.distributed import as_completed
from dask_jobqueue import SLURMCluster
from dask.distributed import Client
from dask.distributed import wait


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

        # python3 baseline_series_v8.py data_path.csv 2008/01/01 2008/01/01 [-90,90,-180,180] [1,1] [5] 1 1 1 1 1 1 1 input_file.csv input_Jhist.csv
        
        # print("usage: python aggre_stats_mpi.py <Data Path> <Start Date> <End Date> \
        #                                         <Polygon boundaries> <Lat & Lon Grid Size > \
        #                                         <Sampling number larger than 0> \
        #                                         <1/0> <1/0> <1/0> \
        #                                         <1/0> <1/0> <1/0> \
        #                                         <1/0> <Variable Imput File> <JHist Variable Imput File>")

        inputSpec = self.request.get('input', [])
        
        
        spl_num = np.int(inputSpec['spl_num'][1:-1])
        poly=np.fromstring(inputSpec['poly'][1:-1], dtype=np.int, sep=',' )
        grid=np.fromstring(inputSpec['grid'][1:-1], dtype=np.float, sep=',' )
        
        # Define the statistics names for HDF5 output
        sts_name = ['Minimum','Maximum','Mean','Pixel_Counts', \
                    'Standard_Deviation','Histogram_Counts','Jhisto_vs_']

        # Pass system arguments to the function
        # sts_switch = np.array(sys.argv[7:14],dtype=np.int)
        sts_switch = np.fromstring(inputSpec['sts_switch'],dtype=np.int, sep=',')
        sts_switch = np.array((sts_switch == 1))
        #varlist = inputSpec['varlist']

        # Read the variable names from the variable name list
        #text_file = np.array(pd.read_csv(varlist, header=0, delim_whitespace=True)) #open(varlist, "r")
        text_file = np.array(pd.DataFrame.from_dict(json.loads(inputSpec['varlist']))) 
        varnames  = text_file[:,0] 

        if sts_switch[5] == True: 
            intervals_1d = text_file[:,1] # This is a string interval arrays
        else: 
            intervals_1d = [0]

        if sts_switch[6] == True:   
            # Read the joint histogram names from the variable name list
            #jvarlist = inputSpec['jvarlist']
            #text_file = np.array(pd.read_csv(jvarlist, header=0, delim_whitespace=True)) #open(varlist, "r")
            text_file = np.array(pd.DataFrame.from_dict(json.loads(inputSpec['jvarlist']))) #open(varlist, "r")
            histnames = text_file[:,1] 
            var_idx   = text_file[:,2] #This is the index of the input variable name which is used for 2D histogram
            intervals_2d = text_file[:,3]
        else:
            intervals_2d,var_idx = [0],[0]

        #-------------STEP 1: Set up the specific directory --------
        # data_path_file = np.array(pd.read_csv(inputSpec['data_path_file'], header=0, delim_whitespace=True))
        data_path_file = np.array(pd.DataFrame.from_dict(json.loads(inputSpec['data_path_file'])))   
        MYD06_dir    = data_path_file[0,0] #'/umbc/xfs1/cybertrn/common/Data/Satellite_Observations/MODIS/MYD06_L2/'
        MYD06_prefix = data_path_file[0,1] #'MYD06_L2.A'
        MYD03_dir    = data_path_file[1,0] #'/umbc/xfs1/cybertrn/common/Data/Satellite_Observations/MODIS/MYD03/'
        MYD03_prefix = data_path_file[1,1] #'MYD03.A'
        fileformat = 'hdf'

        # output_path_file = np.array(pd.read_csv(inputSpec['data_path_file'], header=3, delim_whitespace=True))
        output_path_file = np.array(pd.DataFrame.from_dict(json.loads(inputSpec['output_path']))) 
        output_dir = output_path_file[0,0]
        output_prefix = output_path_file[0,1]

        #-------------STEP 2: Set up spactial and temporal resolution & variable names----------
        NTA_lats = [poly[0],poly[1]] #[  0,40] #[-90,90]   #[-30,30]    
        NTA_lons = [poly[2],poly[3]] #[-40,60] #[-180,180] #[-60,60]  
        
        gap_x, gap_y = grid[1],grid[0] #0.5,0.625

        if ((NTA_lons[-1]-NTA_lons[0])%gap_x != 0) | ((NTA_lats[-1]-NTA_lats[0])%gap_y != 0): 
            print("## ERROR!!!")
            print("Grid size should be dividable by the dimension of the selected region.")
            print("If you choose the region of latitude  from -40 to 40, then you gird size (Latitude ) should be dividable by 80.")
            print("If you choose the region of longitude from  20 to 35, then you gird size (Longitude) should be dividable by 55.")
            print("Please try again!")
            sys.exit()

        map_lon = np.arange(NTA_lons[0],NTA_lons[1],gap_x)
        map_lat = np.arange(NTA_lats[0],NTA_lats[1],gap_y)
        Lon,Lat = np.meshgrid(map_lon,map_lat)
        grid_lon=np.int((NTA_lons[-1]-NTA_lons[0])/gap_x)
        grid_lat=np.int((NTA_lats[-1]-NTA_lats[0])/gap_y)

        #--------------STEP 3: Create arrays for level-3 statistics data-------------------------
        grid_data = {}
        bin_num1 = np.zeros(len(varnames)).astype(np.int)
        bin_num2 = np.zeros(len(varnames)).astype(np.int)
        key_idx = 0
        for key in varnames:
            if sts_switch[0] == True:
                grid_data[key+'_'+sts_name[0]] = np.zeros(grid_lat*grid_lon) + np.inf
            if sts_switch[1] == True:
                grid_data[key+'_'+sts_name[1]] = np.zeros(grid_lat*grid_lon) - np.inf
            if (sts_switch[2] == True) | (sts_switch[3] == True) | (sts_switch[4] == True):
                grid_data[key+'_'+sts_name[2]] = np.zeros(grid_lat*grid_lon)
                grid_data[key+'_'+sts_name[3]] = np.zeros(grid_lat*grid_lon)
                grid_data[key+'_'+sts_name[4]] = np.zeros(grid_lat*grid_lon)
            if sts_switch[5] == True:
                bin_interval1 = np.fromstring(intervals_1d[key_idx], dtype=np.float, sep=',' )
                bin_num1[key_idx] = bin_interval1.shape[0]-1
                grid_data[key+'_'+sts_name[5]] = np.zeros((grid_lat*grid_lon,bin_num1[key_idx]))

                if sts_switch[6] == True:
                    bin_interval2 = np.fromstring(intervals_2d[key_idx], dtype=np.float, sep=',' )
                    bin_num2[key_idx] = bin_interval2.shape[0]-1
                    grid_data[key+'_'+sts_name[6]+histnames[key_idx]] = np.zeros((grid_lat*grid_lon,bin_num1[key_idx],bin_num2[key_idx]))

            key_idx += 1

        #--------------STEP 4: Read the filename list for different time period-------------------
        fname1,fname2 = [],[]

        start_date = np.fromstring(inputSpec['start_date'], dtype=np.int, sep='/' )
        end_date   = np.fromstring(inputSpec['end_date'], dtype=np.int, sep='/' )
        start = date(start_date[0], start_date[1], start_date[2])
        until = date(end_date[0], end_date[1], end_date[2])

        for dt in rrule(DAILY, interval=1, dtstart=start, until=until):
            year  = np.array([np.int(dt.strftime("%Y"))])
            month = np.array([np.int(dt.strftime("%m"))])
            day   = np.array([np.int(dt.strftime("%d"))])
            time  = np.arange(24) #np.int(dt.strftime("%H"))
            
            daynew = dt.toordinal()
            yearstart = datetime(year,1,1)
            yearend   = calendar.monthrange(year, 12)[1]

            day_yearstart = yearstart.toordinal()
            day_yearend = datetime(year,12,yearend).toordinal()

            day_in_year = np.array([(daynew-day_yearstart)+1])
            end_in_year = np.array([(day_yearend-day_yearstart)+1])
            
            # Adjust to 3 hours previous/after the End Date for the orbit gap/overlap problem
            if (dt.year == until.year) & (dt.month == until.month) & (dt.day == until.day):
                shift_hour = 3
                time    = np.append(np.arange(24),np.arange(shift_hour))
                year    = [year[0],year[0]]
                day_in_year = [day_in_year[0],day_in_year[0] + 1]
                if day_in_year[1] > end_in_year:
                    year[1]   -= 1 
                    yearstart = datetime(year[1],1,1)
                    yearend   = datetime(year[1],12,31)
                    day_yearstart = yearstart.toordinal()
                    day_yearend   = yearend.toordinal()
                    day_in_year[1] = (day_yearend-day_yearstart)+1
            
            # Start reading Level-2 files 
            fname_tmp1,fname_tmp2 = series.read_filelist(MYD06_dir,MYD06_prefix,MYD03_dir,MYD03_prefix,year,day_in_year,time,fileformat)
            fname1 = np.append(fname1,fname_tmp1)
            fname2 = np.append(fname2,fname_tmp2)
            
            #print(fname1.shape,fname2.shape)

        filenum = np.arange(len(fname1))
        #print(len(fname1))

        #--------------STEP 5: Read Attributes of each variables----------------------------------
        unit_list = []
        scale_list = []
        offst_list = []
        longname_list = []
        fillvalue_list = []

        ncfile=Dataset(fname1[0],'r')

        # Read the User-defined variables from MYD06 product
        tmp_idx = 0
        for key in varnames:
            if key == 'cloud_fraction': 
                name_idx = tmp_idx
                continue #Ignoreing Cloud_Fraction from the input file
            else:
                tmp_data,data_dim,lonam,unit,fill,scale,offst = series.readEntry(key,ncfile,spl_num)
                unit_list  = np.append(unit_list,unit)
                scale_list = np.append(scale_list,scale)
                offst_list = np.append(offst_list,offst)
                longname_list = np.append(longname_list, lonam)
                fillvalue_list = np.append(fillvalue_list, fill)
                tmp_idx += 1

        # Add the long name of cloud freaction at the first row
        CM_unit     = 'none'
        CM_longname = 'Cloud Fraction from Cloud Mask (cloudy & prob cloudy)'
        CM_fillvalue = -9999
        CM_scale_factor = 0.0001
        CM_add_offset   = 0.0
        unit_list      = np.insert(unit_list,      name_idx, CM_unit)
        scale_list     = np.insert(scale_list,     name_idx, CM_scale_factor)
        offst_list     = np.insert(offst_list,     name_idx, CM_add_offset)
        longname_list  = np.insert(longname_list,  name_idx, CM_longname)
        fillvalue_list = np.insert(fillvalue_list, name_idx, CM_fillvalue)

        ncfile.close()
        #--------------STEP 6: Start Aggregation------------------------------------------------


        xds = self.operate(fname1,fname2,day_in_year,shift_hour,NTA_lats,NTA_lons,grid_lon,grid_lat,gap_x,gap_y,filenum, \
                                    grid_data,sts_switch,varnames,intervals_1d,intervals_2d,var_idx, spl_num, \
                                    sts_name, histnames, bin_num1, bin_num2, year, month, map_lat, map_lon, \
                                    unit_list, scale_list, offst_list, longname_list, fillvalue_list,output_dir, output_prefix)


        #resultDataset = xa.DataArray(xds, name='test')
        return TaskResult(kwargs, [xds])



    def operate(self, fname1,fname2,day_in_year,shift_hour,NTA_lats,NTA_lons,grid_lon,grid_lat,gap_x,gap_y,filenum, \
                                grid_data,sts_switch,varnames,intervals_1d,intervals_2d,var_idx, \
                                spl_num, sts_name, histnames, bin_num1, bin_num2, year, month, map_lat, map_lon, \
                                unit_list, scale_list, offst_list, longname_list, fillvalue_list, output_dir, output_prefix):
        """
            Convenience method defined for this particular operation
        """
      
        request = self.request['operation'][0]['name'].split(':')[1]

        if request == "modisAggr":

            # Start counting operation time
            start_time = timeit.default_timer() 

            # grid_data = series.run_modis_aggre(fname1,fname2,day_in_year,shift_hour,NTA_lats,NTA_lons,grid_lon,grid_lat,gap_x,gap_y,filenum, \
            #                             grid_data,sts_switch,varnames,intervals_1d,intervals_2d,var_idx, spl_num, sts_name, histnames)
                

            # kwargv = {"fname1": fname1, "fname2": fname2, "day_in_year": day_in_year, "shift_hour": shift_hour, "grid_data":grid_data,"NTA_lats": NTA_lats, "NTA_lons": NTA_lons, "grid_lon": grid_lon,"grid_lat": grid_lat, "gap_x": gap_x, "gap_y": gap_y,\
            #  "filenum": filenum, "sts_switch":sts_switch, "varnames": varnames, "intervals_1d":intervals_1d, "intervals_2d":intervals_2d, \
            #  "var_idx":var_idx, "spl_num":spl_num, "sts_name":sts_name, "histnames":histnames}
            # import pdb; pdb.set_trace()
            kwargv = {"day_in_year": day_in_year, "shift_hour": shift_hour, "grid_data":grid_data,"NTA_lats": NTA_lats, "NTA_lons": NTA_lons, "grid_lon": grid_lon,"grid_lat": grid_lat, "gap_x": gap_x, "gap_y": gap_y,\
             "hdfs": filenum, "sts_switch":sts_switch, "varnames": varnames, "intervals_1d":intervals_1d, "intervals_2d":intervals_2d, \
             "var_idx":var_idx, "spl_num":spl_num, "sts_name":sts_name, "histnames":histnames}

            cluster = SLURMCluster(cores=1, memory='64 GB', project='pi_jianwu',\
                queue='high_mem', walltime='16:00:00', job_extra=['--exclusive', '--qos=medium+'])
            print('***********Created cluster************')
            cluster.scale(5)
            print('***********Scaling Done************')
            client = Client(cluster)
            print('***********Created Client************')
            tt = client.map(series.run_modis_aggre, fname1, fname2, **kwargv)
            print('***********Client Mapping Done************')
            num_results= 0
            for future, result in as_completed(tt, with_results= True):
                print("future result " + str(num_results))
                num_results+=1
                #continue
                # print(result)
                # longname_list = result[1]
                # result = result[0]
                # aggregate the result
                # print("grid_lat*grid_lon:")
                # print(grid_lat*grid_lon)

                # for z in np.arange(grid_lat*grid_lon):
                #     # For all variables
                #     key_idx = 0
                #     for key in varnames:
                #         if sts_switch[0] == True:
                #             if  grid_data[key+'_'+sts_name[0]][z] > result[key+'_'+sts_name[0]][z]:
                #                 grid_data[key+'_'+sts_name[0]][z] = result[key+'_'+sts_name[0]][z]
                #         if sts_switch[1] == True:
                #             if  grid_data[key+'_'+sts_name[1]][z] < result[key+'_'+sts_name[1]][z]:
                #                 grid_data[key+'_'+sts_name[1]][z] = result[key+'_'+sts_name[1]][z]
                #         #Total and Count for Mean
                #         if (sts_switch[2] == True) | (sts_switch[3] == True):
                #             grid_data[key+'_'+sts_name[2]][z] += result[key+'_'+sts_name[2]][z]
                #             grid_data[key+'_'+sts_name[3]][z] += result[key+'_'+sts_name[3]][z]
                #         #standard deviation
                #         if sts_switch[4] == True:
                #             grid_data[key+'_'+sts_name[4]][z] += result[key+'_'+sts_name[4]][z]
                #         #1D Histogram
                #         if sts_switch[5] == True:
                #             grid_data[key+'_'+sts_name[5]][z] += result[key+'_'+sts_name[5]][z]
                #         #2D Histogram
                #         if sts_switch[6] == True:
                #             grid_data[key+'_'+sts_name[6]+histnames[key_idx]][z] += result[key+'_'+sts_name[6]+histnames[key_idx]][z]
                #         key_idx += 1

                # for z in np.arange(grid_lat*grid_lon):
                    # For all variables
                key_idx = 0
                for key in varnames:
                    if sts_switch[0] == True:
                        # if  grid_data[key+'_'+sts_name[0]][z] > result[key+'_'+sts_name[0]][z]:
                        grid_data[key+'_'+sts_name[0]] = np.min([grid_data[key+'_'+sts_name[0]],result[key+'_'+sts_name[0]]],axis=0)
                    if sts_switch[1] == True:
                        # if  grid_data[key+'_'+sts_name[1]][z] < result[key+'_'+sts_name[1]][z]:
                        grid_data[key+'_'+sts_name[1]] = np.max([grid_data[key+'_'+sts_name[1]],result[key+'_'+sts_name[1]]],axis=0)
                    #Total and Count for Mean
                    if (sts_switch[2] == True) | (sts_switch[3] == True):
                        grid_data[key+'_'+sts_name[2]] += result[key+'_'+sts_name[2]]
                        grid_data[key+'_'+sts_name[3]] += result[key+'_'+sts_name[3]]
                    #standard deviation
                    if sts_switch[4] == True:
                        grid_data[key+'_'+sts_name[4]] += result[key+'_'+sts_name[4]]
                    #1D Histogram
                    if sts_switch[5] == True:
                        grid_data[key+'_'+sts_name[5]] += result[key+'_'+sts_name[5]]
                    #2D Histogram
                    if sts_switch[6] == True:
                        grid_data[key+'_'+sts_name[6]+histnames[key_idx]] += result[key+'_'+sts_name[6]+histnames[key_idx]]
                    key_idx += 1



            # Compute the mean cloud fraction & Statistics (Include Min & Max & Standard deviation)

            # Reference for statstic parameters
            # sts_name[0]: min
            # sts_name[1]: max
            # sts_name[2]: mean / total_value
            # sts_name[3]: count
            # sts_name[4]: square
            # sts_name[5]: histogram
            # sts_name[6]: joint histogram

            sts_idx = np.array(np.where(sts_switch == True))[0]
            print("Index of User-defined Statistics:",sts_idx)
            key_idx = 0
            for key in varnames:
                for i in sts_idx:
                    if i == 0:
                        grid_data[key+'_'+sts_name[0]] = grid_data[key+'_'+sts_name[0]].reshape([grid_lat,grid_lon])
                    elif i == 1:
                        grid_data[key+'_'+sts_name[1]] = grid_data[key+'_'+sts_name[1]].reshape([grid_lat,grid_lon])
                    elif i == 2:
                        grid_data[key+'_'+sts_name[2]] = (grid_data[key+'_'+sts_name[2]] / grid_data[key+'_'+sts_name[3]])
                        grid_data[key+'_'+sts_name[2]] =  grid_data[key+'_'+sts_name[2]].reshape([grid_lat,grid_lon])
                    elif i == 3:
                        grid_data[key+'_'+sts_name[3]] =  grid_data[key+'_'+sts_name[3]].reshape([grid_lat,grid_lon])
                    elif i == 4:
                        grid_data[key+'_'+sts_name[4]] = ((grid_data[key+'_'+sts_name[4]] / grid_data[key+'_'+sts_name[3]].ravel()) - grid_data[key+'_'+sts_name[2]].ravel()**2)**0.5
                        grid_data[key+'_'+sts_name[4]] =  grid_data[key+'_'+sts_name[4]].reshape([grid_lat,grid_lon])
                    elif i == 5:
                        grid_data[key+'_'+sts_name[5]] = grid_data[key+'_'+sts_name[5]].reshape([grid_lat,grid_lon,bin_num1[key_idx]])
                    elif i == 6:
                        grid_data[key+'_'+sts_name[6]+histnames[key_idx]] = grid_data[key+'_'+sts_name[6]+histnames[key_idx]].reshape([grid_lat,grid_lon,bin_num1[key_idx],bin_num2[key_idx]])
                key_idx += 1    

            end_time = timeit.default_timer()

            #print('Mean_Fraction:')
            #print( Mean_Fraction  )

            print ("Operation Time in {:7.2f} seconds".format(end_time - start_time))

            #--------------STEP 7:  Create HDF5 file to store the result------------------------------
            l3name  = output_prefix + '.A{:04d}{:03d}.'.format(year[0],day_in_year[0])

            subname = 'serial_output_daily_1km_v3.h5'
            #ff=h5py.File(output_dir+l3name+subname,'w')
            ff=h5py.File(l3name+subname,'w')

            PC=ff.create_dataset('lat_bnd',data=map_lat)
            PC.attrs['units']='degrees'
            PC.attrs['long_name']='Latitude_boundaries'    

            PC=ff.create_dataset('lon_bnd',data=map_lon)
            PC.attrs['units']='degrees'
            PC.attrs['long_name']='Longitude_boundaries'    

            for i in range(sts_idx.shape[0]):
                cnt = 0
                for key in grid_data:

                    if key.find("1km") != -1: 
                        new_name = key.replace("_1km", "")
                    else: 
                        new_name = key

                    if (sts_name[sts_idx[i]] in key) == True:  
                        #print(sts_name[sts_idx[i]],key,grid_data[key].shape)
                        #print(longname_list[cnt][:20],new_name)
                        series.addGridEntry(ff,new_name,unit_list[cnt],longname_list[cnt],fillvalue_list[cnt],scale_list[cnt],offst_list[cnt],grid_data[key],intervals_1d[cnt],intervals_2d[cnt])
                        cnt += 1
            
            
            
            ff.close()

            xds = xa.open_dataset(l3name+subname)
            xds.load()
            print(l3name+subname+' Saved!')

            # convert h5py to xarray

        else:
            raise Exception(f"Unknown operation: '{request}'")
        return xds
