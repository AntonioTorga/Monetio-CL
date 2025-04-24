from abc import abstractmethod
from pathlib import Path
from dask import dataframe as dd
from dask.diagnostics import ProgressBar
import xarray as xr
import re


class Translator:
    """
    Translates data from raw to intermediate format, and intermediate to netcdf 
    (Melodies-Monet ready) format.
    """
    def __init__(self, input_path, output_path, station_fn, **kwargs):
        self.input_path = Path(input_path)
        self.intermediate_path = kwargs["intermediate_path"] if kwargs["intermediate_path"] else self.input_path / "intermediate"
        self.output_path = Path(output_path)

        self.station_fn = kwargs["station_fn"] if kwargs["station_fn"] else "station.csv"

        file_info = {
            "input_file" : {
                "regex": kwargs["input_file_regex"] if kwargs["input_file_regex"] else "(\d*)-(Met|Cal)-(\d*)-(\d*).json",
                },
            "intermediate_file" : {
                "regex": kwargs["intermediate_file_regex"] if kwargs["intermediate_file_regex"] else "(\d+)--(.*)\.csv",
                "format": kwargs["intermediate_file_format"] if kwargs["intermediate_file_format"] else "{0}--{1}.csv",
                },
            "output_file" : {
                "format": kwargs["output_file_format"] if kwargs["output_file_format"] else "{0}--{1}.nc",
                },
        }

    @abstractmethod
    def raw_to_intermediate_file(self, file):
        pass
    
    def load_intermediate_data(self) -> dd:
        """
        Loads the intermediate data from the intermediate path.
        Assumes all the data must be merged into a single dataframe.
        """
        files = [f for f in Path(self.intermediate_path).iterdir() if f.is_file() and re.search(self.intermediate_fn_format, f.name)]        
        
        ddfs = []
        for file in files:
            # load the data
            _ddf = dd.read_csv(file, sep=",", decimal=".")
            ddfs+=_ddf
        
        station_df = dd.read_csv(self.intermediate_path / self.station_fn, sep=",", decimal=".")

        return ddfs, station_df

    def intermediate_to_xarray(self, ddfs, station_df):
        """
        Converts the intermediate data to xarray format.
        ddfs: list of dask dataframes for each station.
        """
        ddf = dd.DataFrame()
        for ddf_ in ddfs:
            
            ddf = ddf.merge(ddf_, how="outer")
        
        ds = (ddf.set_index(["siteid", "time"])
            .to_xarray()
            .swap_dims(siteid="x")
            .set_coords(["latitude", "longitude", "elevation"]) #elevation might be extra
            .assign(x=range(ddf["siteid"].nunique()))
            .expand_dims("y")
            .transpose("time", "y", "x")
        )

        return ds
    
    def raw_to_intermediate_format(self, input_fn_format = None, save=False):
        # TODO: check that is correct
        # ASSUMES: the input path has raw data in the format of the input_fn_format that
        # is meant for a single intermediate file.
        # For MULTIPLE ones, we must call this function for every different filename format.
        # turn all files with the filename format to intermediate format
        # for each file in the input path, load the data and process it

        input_fn_format = input_fn_format if input_fn_format else self.input_fn_format

        ddf = dd.DataFrame()

        files = [f for f in Path(self.input_path).iterdir() if f.is_file() and re.search(input_fn_format, f.name)]        

        for file in files:
            # load the data
            _ddf= self.raw_to_intermediate_ddf(file)
            ddf.merge(_ddf, how="outer")

        if save:
            # save the data to the output path
            ddf.to_csv(self.intermediate_path/ self.intermediate_fn_format, index=False, sep=",", decimal=".")
        
        return ddf

    def xarray_to_netcdf(self, xarray):
            """
            Save the xarray dataset to netcdf format in the output path.
            """
            if self.output_path.is_dir() == False:
                self.output_path.mkdir(parents=True, exist_ok=True)

            xarray.to_netcdf(self.output_path / f"{self.output_fn}.nc", format="NETCDF4", unlimited_dims="time" )

    def from_intermediate_to_netcdf(self):
        """
        Converts the intermediate data to netcdf format.
        """
        ddfs, station_df = self.load_intermediate_data()
        xarray = self.intermediate_to_xarray(ddfs, station_df)
        self.xarray_to_netcdf(xarray)

    def from_raw_to_netcdf(self, save=False):
        # TODO: fix logic to adapt to multiple intermediate files.
        """
        Converts the raw data to netcdf format.
        """
        # load the data from the input path
        ddf = self.raw_to_intermediate_format(save=save)
        # convert the data to xarray format
        xarray = self.intermediate_to_xarray(ddf)
        # save the data to netcdf format
        self.xarray_to_netcdf(xarray)