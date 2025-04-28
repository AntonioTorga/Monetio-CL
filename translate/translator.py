from abc import abstractmethod
from pathlib import Path
from dask import dataframe as dd
from dask.diagnostics import ProgressBar
import xarray as xr
import re
import json


class Translator:
    """
    Translates data from raw to intermediate format, and intermediate to netcdf 
    (Melodies-Monet ready) format.
    """
    def __init__(self, input_path, output_path, **kwargs):
        self.input_path = Path(input_path)
        self.intermediate_path = kwargs["intermediate_path"] if kwargs["intermediate_path"] else self.input_path / "intermediate"
        self.output_path = Path(output_path)

        #TODO: move info to a config file
        self.file_info = {
            "input_file" : {
                "regex": kwargs["input_file_regex"] if kwargs["input_file_regex"] else "(\d*)-(Met|Cal)-(\d*)-(\d*).json", # IDStation-Type-Year-Month
                },
            "intermediate_file" : {
                "regex":  "(\d+)--(.*)\.csv", # IDStation-Type
                "format": "{0}--{1}.csv", # IDStation-Type
                },
            "output_file" : {
                "format": kwargs["output_file_format"] if kwargs["output_file_format"] else "{0}--{1}.nc", # Network-Type
                },
            "station_file" : {
                "raw_regex" : kwargs["station_file_name"]+".json" if kwargs["station_file_name"] else "stations.json",
                "intermediate_regex" : kwargs["station_file_name"]+".csv" if kwargs["station_file_name"] else "stations.csv",
            }
        }

    @abstractmethod
    def raw_to_intermediate_file(self, files, filename, save=False):
        pass

    @abstractmethod
    def raw_to_intermediate_station_format(self):
        pass
    
    def load_intermediate_data(self):
        """
        Loads the intermediate data from the intermediate path.
        Assumes all the data must be merged into a single dataframe.
        """
       # files = [f for f in Path(self.intermediate_path).iterdir() if f.is_file() and re.search(self.file_info["intermediate_file"]["regex"], f.name)]        
        data = {}
        for f in Path(self.intermediate_path).iterdir():
            match = re.search(self.file_info["intermediate_file"]["regex"], f.name)
            if f.is_file() and match:
                # get the station id and type from the filename
                station_id = match.group(1)
                station_type = match.group(2)
                data[station_id] = {
                    "station_id": station_id,
                    "station_type": station_type,
                    "file": f, 
                    "data": dd.read_csv(f, sep=",", decimal=".")
                }
        
        station_df = dd.read_csv(self.intermediate_path / self.file_info["station_file"]["intermediate_regex"], sep=",", decimal=".")

        return data, station_df

    def intermediate_to_xarray(self, data, station_df):
        """
        Converts the intermediate data to xarray format.
        ddfs: list of dask dataframes for each station.
        """
        ddf = dd.from_dict({}, npartitions=10)

        for station_id, info in data.items():
            ddf_ = info["data"]
            ddf_["siteid"] = station_id
            station_data = station_df.loc[station_id, ["latitud", "longitud", "altura"]].compute()
            ddf_["latitude"] = station_data["latitud"].iloc[0]
            ddf_["longitude"] = station_data["longitud"].iloc[0]
            ddf_["elevation"] = station_data["altitud"].iloc[0]
            ddf_ = ddf_.rename(columns={"momento": "time"})
            # cast all columns to string
            ddf_ = ddf_.astype(str)

            ddf = ddf.merge(ddf_, how="outer")
            
        # TODO: fix this part, doesn't work with dask dataframe
        ds = (ddf.set_index(["siteid", "time"])
            .to_xarray()
            .swap_dims(siteid="x")
            .set_coords(["latitude", "longitude", "elevation"]) #elevation might be extra
            .assign(x=range(ddf["siteid"].nunique()))
            .expand_dims("y")
            .transpose("time", "y", "x")
        )

        return ds
    
    def xarray_to_netcdf(self, xarray):
            """
            Save the xarray dataset to netcdf format in the output path.
            """
            if self.output_path.is_dir() == False:
                self.output_path.mkdir(parents=True, exist_ok=True)

            xarray.to_netcdf(self.output_path / (self.output_fn+".nc"), format="NETCDF4", unlimited_dims="time" )

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
    
    def from_intermediate_to_netcdf(self):
        """
        Converts the intermediate data to netcdf format.
        """
        ddfs, station_df = self.load_intermediate_data()
        xarray = self.intermediate_to_xarray(ddfs, station_df)
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