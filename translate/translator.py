
from pathlib import Path
from dask import dataframe as dd

class NetcdfTranslator:
    def __init__(self, input_path, output_path, filename_format):
        self.filename_format = filename_format

        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.data = None

    def load_file(self, file):
        pass

    def load_multifile(self):
        for file in self.input_path.glob(self.filename_format):
            self.data = self.load_file(file)

    def load_data(self):
        self.load_multifile()
        return self.data
    
    def to_netcdf(self):
        pass
    
    def run(self):
        self.load_data()
        self.to_netcdf()
        return self.data