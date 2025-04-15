from abc import abstractmethod
from pathlib import Path
from dask import dataframe as dd

class NetcdfTranslator:
    """Base class for translating data files to NetCdf format.
    Accumulates data in self.data and then proceeds to save it in NetCDF format.
    The data is loaded from the input_path and saved to the output_path.
    The filename_format is used to match the files in the input_path.
    The input_path and output_path are expected to be directories.
    """
    def __init__(self, input_path, output_path, station_filename,
                 filename_format, name_template=None):
        self.filename_format = filename_format

        self.input_path = Path(input_path).absolute()
        self.output_path = Path(output_path).absolute()
        self.data = dict()
        self.station_info = dict()
    @abstractmethod
    def setup_data(self):
        pass

    @abstractmethod
    def process_df(self, df):
        pass
    
    @abstractmethod
    def load_file(self, file):
        pass

    @abstractmethod
    def get_station_info(self):
        pass
    
    @abstractmethod
    def load_data(self):
        pass
    
    def data_to_xarray(self, ddf):
        # create the right dimensions, vars and coords for the xarray dataset
        return ddf.to_xarray()
        
    def to_netcdf(self):
        for typ in self.data:
            #convert data to xarray
            data = typ["data"]
            xr = self.data_to_xarray(data)
            xr.to_netcdf(self.output_path / f"{typ["out_name"]}.nc", format="NETCDF4", unlimited_dims="time")

    def run(self):
        self.load_data()
        self.to_netcdf()

        return self.data