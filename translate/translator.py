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
    def __init__(self, intermediate_path, output_path, verbose=False,**kwargs):
        self.intermediate_path = intermediate_path
        self.output_path = output_path
        self.timestep = kwargs.get("timestep")
        self.verbose = verbose

        #TODO: move info to a config file
        self.file_info = {
            "input_file" : {
                "regex": kwargs.get("input_file_regex", "(\\d*)-(\\d*)-(\\d*).json") # IDStation-Year-Month
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
            _ddf = dd.from_pandas(self._to_pd_dataframe(file),npartitions=1)
            ddf = _ddf if ddf is None else dd.concat([ddf,_ddf])
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
                    "file": f, 
                    "data": ddf
                }
        station_ddf = dd.read_csv(self.intermediate_path / self.file_info["station_file"]["intermediate_regex"], sep=",", decimal=".")
        station_ddf = self.preprocess_intermediate_station_data(station_ddf, id_name, lat_name, lon_name)

        return data, station_ddf

    def load_raw_data(self):
        filename_regex = self.file_info["input_file"]["regex"]
        station_filename_regex= self.file_info["station_file"]["raw_regex"]
        # get the list of files in the input path that match the regex
        file_list = sorted(Path(self.input_path).iterdir(), key=lambda entry: entry.name)
        
        stations = {} # dictionary to store the raw files that compose each intermediate file for a station.
        for f in file_list:
            match = re.search(filename_regex, f.name)
            if f.is_file() and match:
                # get the station id and type from the filename
                station_id = match.group(1)
                station_info = match.group(2)
                year = match.group(3)
                month = match.group(4)

                if stations.get(station_id) == None:
                    stations[station_id] = {
                        "station_id": station_id,
                        "station_intermediate_filename": self.file_info["intermediate_file"]["format"].format(station_id),
                        "files": [],
                    }
                stations[station_id]["files"].append(f)
        
        raw_station = 
        
        return stations, raw_station
    
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
    
    def postprocess_xarray_data(self, xrds, time_interval):
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
        return xrds
    
    def intermediate_to_xarray(self, data, station_ddf):
        """
        Converts the intermediate data to xarray format.
        ddfs: list of dask dataframes for each station.
        """
        data_dd = []
        site_id_dim = []
        ## test
        nombre_dim = []
        region_dim = []
        comuna_dim = []
        provincia_dim = []
        clase_dim = []
        ##
        lat_dim = []
        lon_dim = []
        timestamps = set()
        columns = set()

        for site_id, data_dict in data.items():
            _ddf = data_dict["data"]
            file = data_dict["file"]
            try:
                id_row = station_ddf.loc[site_id].compute()
            except KeyError: 
                if self.verbose:
                    print(f"No row matching the id {site_id} in station file info.")
                continue
            lat, lon = list(id_row[["latitude","longitude"]].iloc[0])

            ## test
            nombre, region, comuna, provincia, clase = list(id_row[["Nombre", "Nombre Region", "Comuna", "Provincia", "Clase"]].iloc[0])
            ##

            # Dask dataframe is now correctly indexed and in a regular format.
            data_dd.append(_ddf)
            site_id_dim.append(site_id)

            ## test
            nombre_dim.append(nombre)
            region_dim.append(region)
            comuna_dim.append(comuna)
            provincia_dim.append(provincia)
            clase_dim.append(clase)

            ##
            lat_dim.append(lat)
            lon_dim.append(lon)

            columns.update(_ddf.columns.to_list())
            timestamps.update(_ddf.index.compute().to_list())
            
            if self.verbose:
                print(f"File {file.name} processed.")
        
        timestamps = sorted(list(timestamps))

        if self.verbose:
            print(f"Computing dask dataframes into pandas dataframes.")
        data_df = [ddf.compute().reindex(timestamps, fill_value=np.nan) for ddf in data_dd]
        
        data_vars = create_data_vars_dict(data_df, timestamps, columns)
        if self.verbose:
            print("Creating xarray dataset...")
        
        xrds = xr.Dataset(
                data_vars,
                coords = {
                    "time": ("time", timestamps),
                    "latitude": ("x", lat_dim),
                    "longitude": ("x", lon_dim),
                    "siteid":("x", list(map(str,site_id_dim))),
                    "region":("x", region_dim),
                    "comuna": ("x", comuna_dim),
                    "nombre": ("x", nombre_dim),
                    "provincia":("x", provincia_dim),
                    "clase":("x",clase_dim),
                    "x": ("x", np.arange(len(site_id_dim)))
                },
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

            if self.output_path.is_dir() == False:
                self.output_path.mkdir(parents=True, exist_ok=True)

            xarray.to_netcdf(file_path, format="NETCDF4", unlimited_dims="time" )

    def from_raw_to_intermediate_format(self, raw_data=None, raw_station=None, save=False):
        """
        Converts the raw data to intermediate format.
        Collects all the raw files that go into each intermediate station file.
        """
        if raw_data==None:
        
        ddfs = {} # dictionary that will hold the dask dataframes for each station.
        for station_id in stations.keys():
            files = stations[station_id]["files"]
            intermediate_filename = stations[station_id]["station_intermediate_filename"]

            data = self.raw_to_intermediate_file(files)
            if save:
                data.to_csv(intermediate_filename, single_file=True)

            ddfs[station_id] = {
                "station_id": station_id,
                "station_filename": intermediate_filename,
                "data": data,
            }

        station_ddf = self.raw_to_intermediate_station(raw_station)

        if save:
            station_ddf.to_csv(self.intermediate_path/self.file_info["station_file"]["intermediate_format"])
        
        return ddfs, station_ddf
    
    def from_intermediate_to_netcdf(self, time_name, id_name, lat_name, lon_name, data=None, station_ddf=None):
        """
        Converts the intermediate data to netcdf format.
        """
        if data==None or station==None:
            data, station_ddf = self.load_intermediate_data(time_name, id_name, lat_name, lon_name)
        xarray = self.intermediate_to_xarray(data, station_ddf)
        self.xarray_to_netcdf(xarray)

    def from_raw_to_netcdf(self, raw_data=None, save=False):
        """
        Converts the raw data to netcdf format.
        """
        # load the data from the input path
        ddfs, station_ddf = self.from_raw_to_intermediate_format(save=save)
        # convert the data to xarray format
        xarray = self.intermediate_to_xarray(ddfs, station_ddf)
        # save the data to netcdf format
        self.xarray_to_netcdf(xarray)