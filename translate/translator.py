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
    def __init__(self, input_path, output_path, filename_format):
        self.filename_format = filename_format

        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.data = None

    @abstractmethod
    def process_df(self, df):
        pass
    
    @abstractmethod
    def load_file(self, file):
        pass

    def load_multifile(self):
        for file in self.input_path.glob(self.filename_format):
            self.data = self.load_file(file)

    def load_data(self):
        self.load_multifile()
        return self.data
    
    def to_netcdf(self):
        self.data.to_netcdf(self.output_path / f"{self.input_path.stem}.nc", format="NETCDF4", unlimited_dims="time")
    
    def run(self):
        self.load_data()
        self.to_netcdf()
        return self.data