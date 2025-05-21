from abc import abstractmethod
from pathlib import Path
from dask import dataframe as dd
from dask.diagnostics import ProgressBar
import xarray as xr
import numpy as np
import re
import json
from utils.utils import to_float

class Translator:
    """
    Translates data from raw to intermediate format, and intermediate to netcdf 
    (Melodies-Monet ready) format.
    """
    def __init__(self, intermediate_path, output_path, verbose=False,**kwargs):
        self.intermediate_path = Path(intermediate_path)
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.verbose = verbose

        #TODO: move info to a config file
        self.file_info = {
            "input_file" : {
                "regex": kwargs["input_file_regex"] if kwargs.get("input_file_regex") else "(\\d*)-(Met|Cal)-(\\d*)-(\\d*).json", # IDStation-Type-Year-Month
                },
            "intermediate_file" : {
                "regex": kwargs.get("intermediate_filename_regex") if kwargs.get("intermediate_filename_regex") else r"(\d+)--(.*).csv", # IDStation-Type
                "format": "{0}--{1}.csv", # IDStation-Type
                },
            "output_file" : {
                "format": kwargs["output_name"] if kwargs.get("output_name") else "noname_nc_data.nc", # Network-Type
                },
            "station_file" : {
                "raw_regex" : kwargs["raw_station_filename"]+".json" if kwargs.get("raw_station_filename") else "stations.json",
                "intermediate_regex" : kwargs["station_filename"]+".csv" if kwargs.get("station_filename") else "stations.csv",
            }
        }

    @abstractmethod
    def raw_to_intermediate_file(self, files, filename, save=False):
        pass

    @abstractmethod
    def raw_to_intermediate_station_format(self):
        pass
    
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
    
    def preprocess_intermediate_station_data(self, station_ddf, id_name, lat_name, lon_name):
        for column in [id_name, lat_name, lon_name]:
            if column not in station_ddf.columns:
                raise ValueError(f"No {column} column in Station file.")
        station_ddf = station_ddf.rename(columns={id_name: "site_id", lat_name:"latitude", lon_name:"longitude"})
        station_ddf = station_ddf.set_index("site_id", sorted=True)
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
        ddf = ddf.set_index("time", sorted=True)

        return ddf

    def create_data_vars_dict(self, data_df, timestamps, columns):
        data_vars = {}
        for col in columns:
            data_vars[col] =  (["x","time"], [])
            for i, df in enumerate(data_df):
                if col in df.columns.to_list():
                    data_vars[col][1].append(df[col].to_numpy())
                else: 
                    data_vars[col][1].append(np.full(len(timestamps), np.nan))
        return data_vars
    
    def intermediate_to_xarray(self, data, station_ddf):
        """
        Converts the intermediate data to xarray format.
        ddfs: list of dask dataframes for each station.
        """
        data_dd = []
        site_id_dim = []
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

            # Dask dataframe is now correctly indexed and in a regular format.
            data_dd.append(_ddf)
            site_id_dim.append(site_id)
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
        
        data_vars = self.create_data_vars_dict(data_df, timestamps, columns)
        if self.verbose:
            print("Creating xarray dataset...")
        
        xrds = xr.Dataset(
                data_vars,
                coords = {
                    "time": ("time", timestamps),
                    "site_id": ("x", site_id_dim),
                    "latitude": ("x", lat_dim),
                    "longitude": ("x", lon_dim),
                    "x": ("x", np.arange(len(site_id_dim)))
                },
        ).expand_dims("y").set_coords(["site_id", "latitude", "longitude"])
        xrds = xrds.transpose("time","y","x")

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

    def from_raw_to_intermediate_format(self, save=False):
        """
        Converts the raw data to intermediate format.
        Collects all the raw files that go into each intermediate station file.
        """
        filename_regex = self.file_info["input_file"]["regex"]

        # get the list of files in the input path that match the regex
        file_list = sorted(Path(self.input_path).iterdir(), key=lambda entry: entry.name)
        
        stations = {} # dictionary to store the raw files that compose each intermediate file for a station.
        for f in file_list:
            match = re.search(filename_regex, f.name)
            if f.is_file() and match:
                # get the station id and type from the filename
                station_id = match.group(1)
                station_type = match.group(2)
                year = match.group(3)
                month = match.group(4)

                if stations.get(station_id) == None:
                    stations[station_id] = {
                        "station_id": station_id,
                        "station_intermediate_filename": self.file_info["intermediate_file"]["format"].format(station_id, station_type),
                        "files": [],
                    }
                stations[station_id]["files"].append(f)
        
        ddfs = {} # dictionary that will hold the dask dataframes for each station.
        for station_id in stations.keys():
            files = stations[station_id]["files"]
            intermediate_filename = stations[station_id]["station_intermediate_filename"]

            data = self.raw_to_intermediate_file(files, intermediate_filename, save=save)
            ddfs[station_id] = {
                "station_id": station_id,
                "station_filename": intermediate_filename,
                "data": data,
            }

        station_ddf = self.raw_to_intermediate_station_format()
        
        return ddfs, station_ddf
    
    def from_intermediate_to_netcdf(self, time_name, id_name, lat_name, lon_name):
        """
        Converts the intermediate data to netcdf format.
        """
        data, station_ddf = self.load_intermediate_data(time_name, id_name, lat_name, lon_name)
        xarray = self.intermediate_to_xarray(data, station_ddf)
        self.xarray_to_netcdf(xarray)

    def from_raw_to_netcdf(self, save=False):
        """
        Converts the raw data to netcdf format.
        """
        # load the data from the input path
        ddfs, station_ddf = self.from_raw_to_intermediate_format(save=save)
        # convert the data to xarray format
        xarray = self.intermediate_to_xarray(ddfs, station_ddf)
        # save the data to netcdf format
        self.xarray_to_netcdf(xarray)