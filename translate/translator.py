from abc import abstractmethod
from pathlib import Path
from dask import dataframe as dd
from dask.diagnostics import ProgressBar
import xarray as xr
import numpy as np
import re
import json
from utils.utils import to_float, create_data_vars_dict

class Translator:
    """
    Translates data from raw to intermediate format, and intermediate to netcdf 
    (Melodies-Monet ready) format.
    """
    def __init__(self, intermediate_path, output_path, **kwargs):
        self.raw_path = kwargs.get("raw_path")
        self.intermediate_path = intermediate_path
        self.output_path = output_path
        self.timestep = kwargs.get("timestep")
        self.verbose = kwargs.get("verbose", False)

        #TODO: move info to a config file
        self.file_info = {
            "input_file" : {
                "regex": kwargs.get("input_file_regex", "(\\d*)-(\\d*)-(\\d*).json") # Siteid-Year-Month
                },
            "intermediate_file" : {
                "regex": kwargs.get("intermediate_filename_regex", r"(\d+).csv"), # IDStation
                "format": "{siteid}.csv", # IDStation-Info
                },
            "output_file" : {
                "format": kwargs.get("output_name", "noname_nc_data.nc") # Network-Type
                },
            "station_file" : {
                "raw_regex" : kwargs.get("raw_station_filename", "stations.json"),
                "intermediate_format" : kwargs.get("station_filename",  "stations.csv"),
            }
        }
    
    @abstractmethod
    def _to_pd_dataframe(self, file):
        pass

    @abstractmethod
    def raw_to_intermediate_station(self, station_data):
        pass

    def raw_to_intermediate_file(self, files):
        ddf = None
        for file in files:
            pd_dataframe = self._to_pd_dataframe(file)
            if not pd_dataframe.empty:
                _ddf = dd.from_pandas(pd_dataframe,npartitions=1)
                ddf = _ddf if ddf is None else dd.concat([ddf,_ddf])
        if ddf is None:
            ddf = dd.from_dict({},npartitions=1)
        return ddf

    def load_intermediate_data(self, time_name, id_name, lat_name, lon_name):
        """
        Loads the intermediate data from the intermediate path.
        Assumes all the data must be merged into a single dataframe.
        """
        data = {}
        for f in Path(self.intermediate_path).iterdir():
            match = re.search(self.file_info["intermediate_file"]["regex"], f.name)            
            if f.is_file() and match:
                # get the station id and type from the filename
                site_id = int(match.group(1))
                ddf = dd.read_csv(f, sep=",", decimal=".")
                try:
                    ddf = self.preprocess_intermediate_data(ddf, time_name, site_id)
                except ValueError as e:
                    continue
                data[site_id] = {
                    "site_id": site_id,
                    "data": ddf
                }
        station_ddf = dd.read_csv(self.intermediate_path / self.file_info["station_file"]["intermediate_regex"], sep=",", decimal=".")
        station_ddf = self.preprocess_intermediate_station_data(station_ddf, id_name, lat_name, lon_name)

        return data, station_ddf

    def load_raw_data(self):
        filename_regex = self.file_info["input_file"]["regex"]
        station_filename_regex= self.file_info["station_file"]["raw_regex"]
        station_file = None
        # get the list of files in the input path that match the regex
        file_list = sorted(Path(self.input_path).iterdir(), key=lambda entry: entry.name)
        
        raw_data = {} # dictionary to store the raw files that compose each intermediate file for a station.
        for f in file_list:
            match = re.search(filename_regex, f.name)
            if f.is_file() and match:
                # get the station id and type from the filename
                station_id = match.group(1)
                year = match.group(2)
                month = match.group(3)
                data = None
                with open(f) as fp:
                    data = json.load(fp)

                if raw_data.get(station_id) == None:
                    raw_data[station_id] = []
                raw_data[station_id].append({"info": {"siteid":station_id, "year": year, "month": month},"data": data})
            elif match_station :=  re.search(station_filename_regex, f.name):
                with open(f) as fp:
                    raw_station = json.load(fp)
        
        return raw_data, raw_station
    
    def preprocess_intermediate_station_data(self, station_ddf, id_name, lat_name, lon_name):
        for column in [id_name, lat_name, lon_name]:
            if column not in station_ddf.columns:
                raise ValueError(f"No {column} column in Station file.")
        station_ddf = station_ddf.rename(columns={id_name: "siteid", lat_name:"latitude", lon_name:"longitude"})
        station_ddf = station_ddf.set_index("siteid", sorted=True)
        return station_ddf

    def preprocess_intermediate_data(self, ddf, time_name, site_id):
        if not time_name in ddf.columns:
            raise ValueError(f"No column {time_name} in file for {site_id} site.")
        rename = {time_name:"time"}
        cols = ddf.columns.to_list() 
        cols.remove(time_name)  
        if cols:
            for col in cols:
                pos_replacement = col.replace("/", "|")
                if col != pos_replacement:
                    rename[col] = pos_replacement
                if ddf[col].dtype not in ["float64", "int64", "int32", "float32"]:
                        ddf[col] = ddf[col].apply(to_float, meta=float)
        ddf = ddf.rename(columns = rename)
        ddf = ddf.dropna(subset=["time"])
        ddf = ddf.drop_duplicates(subset=["time"])
        ddf['time']= dd.to_datetime(ddf.time)
        ddf = ddf.set_index("time", sorted=True)

        return ddf
    
    def postprocess_xarray_data(self, xrds, time_interval="N", start=None, end=None):
        dict_intervals = {
            "H":"1h",
            "h":"1h",
            "D":"1D",
            "M":"1M",
            "Y":"1Y",
            "N": None
        }
        time_interval = dict_intervals[time_interval]
        if time_interval != None:
            xrds = xrds.resample(time=time_interval).mean()

        xrds = xrds.set_coords(["siteid", "latitude", "longitude"])

        if start!=None and end!=None:
            xrds = xrds.sel(time=slice(start, end))
    
        return xrds
    
    def intermediate_to_xarray(self, data, station_ddf, location_attr_names=[]):
        """
        Converts the intermediate data to xarray format.
        ddfs: list of dask dataframes for each station.
        """
        data_dd = []
        site_id_dim = []
        lat_dim = []
        lon_dim = []
        location_attr_dims = {attribute:[] for attribute in location_attr_names}
        timestamps = set()
        columns = set()

        for site_id, data_dict in data.items():
            _ddf = data_dict["data"]
            try:
                id_row = station_ddf.loc[site_id].compute()
            except KeyError: 
                if self.verbose:
                    print(f"No row matching the id {site_id} in station file info.")
                continue
            lat, lon = list(id_row[["latitude","longitude"]].iloc[0])

            data_dd.append(_ddf)
            site_id_dim.append(site_id)
            lat_dim.append(lat)
            lon_dim.append(lon)

            #support for location attributes provided by the user
            for attribute in location_attr_dims.keys():
                x = id_row[[attribute]].iloc[0][attribute]
                location_attr_dims[attribute].append(x)
        
            columns.update(_ddf.columns.to_list())
            timestamps.update(_ddf.index.compute().to_list())
            
            if self.verbose:
                print(f"Site {site_id} processed.")
        
        timestamps = sorted(list(timestamps))

        if self.verbose:
            print(f"Computing dask dataframes into pandas dataframes.")
        data_df = [ddf.compute().reindex(timestamps, fill_value=np.nan) for ddf in data_dd]
        
        if self.verbose:
            print("Creating xarray dataset...")
        data_vars = create_data_vars_dict(data_df, timestamps, columns)
        coords = {
                    "time": ("time", timestamps),
                    "latitude": ("x", lat_dim),
                    "longitude": ("x", lon_dim),
                    "siteid":("x", list(map(str,site_id_dim))),
                    "x": ("x", np.arange(len(site_id_dim)))
                }
        if len(location_attr_names)>0:
            coords.update({attribute:("x", attribute_dim) for attribute, attribute_dim in location_attr_dims.items()})

        xrds = xr.Dataset(
                data_vars,
                coords = coords,
        )

        if self.timestep:
            if self.verbose:
                print("Postprocessing... changing to correct timestep.")
            xrds = self.postprocess_xarray_data(xrds, self.timestep)

        if self.verbose:
            print("Quick result inspection: ")
            print(xrds)
        
        return xrds

    def xarray_to_netcdf(self, xarray):
            """
            Save the xarray dataset to netcdf format in the output path.
            """
            file_path = self.output_path / (self.file_info["output_file"]["format"])
            if self.verbose:
                print(f"Writing to resulting netcdf dataset to {file_path}")


            xarray.to_netcdf(file_path, format="NETCDF4", unlimited_dims="time" )

    def from_raw_to_intermediate_format(self, raw_data=None, raw_station=None, save=False, merge=False, time_name=None):
        """
        Converts the raw data to intermediate format.
        Collects all the raw files that go into each intermediate station file.
        """
        if raw_data==None or raw_station==None:
            if self.raw_path == None:
                raise FileNotFoundError("No raw path provided to load raw data...")
            raw_data, raw_station = self.load_raw_data()

        
        ddfs = {} # dictionary that will hold the dask dataframes for each station.
        for station_id, station_data in raw_data.items():

            data = self.raw_to_intermediate_file(station_data)
            # if there is no data available for a station, we skip said station
            if len(data.index)==0:
                print(f"No data for station {station_id}")
                continue

            if save:
                intermediate_filepath = self.intermediate_path / self.file_info["intermediate_file"]["format"].format(**{"siteid":station_id})
                # TODO: add data completion if merge is True
                data.to_csv(intermediate_filepath, single_file=True)

            ddfs[station_id] = {
                "station_id": station_id,
                "data": data,
            }

        station_ddf = self.raw_to_intermediate_station(raw_station)

        if save:
            station_ddf.to_csv(self.intermediate_path/self.file_info["station_file"]["intermediate_format"], single_file=True)
        
        return ddfs, station_ddf

    def from_intermediate_to_netcdf(self, time_name, id_name, lat_name, lon_name, data=None, station_ddf=None):
        """
        Converts the intermediate data to netcdf format.
        """
        if data==None or station_ddf==None:
            data, station_ddf = self.load_intermediate_data(time_name, id_name, lat_name, lon_name)
        xarray = self.intermediate_to_xarray(data, station_ddf)
        self.xarray_to_netcdf(xarray)

    def from_raw_to_netcdf(self, raw_data=None, raw_station=None, time_name="momento", lat_name= "latitud",
                            lon_name="longitud", id_name="codigoNacional", location_attr_names=[],
                            start=None, end=None, time_interval= "N",
                            save=False, merge=False):
        """
        Converts the raw data to netcdf format.
        """
        # load the data from the input path
        ddfs, station_ddf = self.from_raw_to_intermediate_format(raw_data, raw_station, save = save, merge=merge, time_name=time_name)
        # convert the data to xarray format

        for site_id, ddf in ddfs.items():
            ddfs[site_id]["data"] = self.preprocess_intermediate_data(ddf["data"], time_name, site_id=site_id)

        station_ddf = self.preprocess_intermediate_station_data(station_ddf, id_name, lat_name, lon_name)
        xarray = self.intermediate_to_xarray(ddfs, station_ddf, location_attr_names=location_attr_names)
        xarray = self.postprocess_xarray_data(xarray, time_interval= time_interval, start=start, end=end)
        # save the data to netcdf format
        self.xarray_to_netcdf(xarray)