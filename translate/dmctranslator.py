from translator import NetcdfTranslator

class DmcToNetCdf(NetcdfTranslator):
    """Class for translating DMC data to NetCDF format.
    """
    def __init__(self, input_path, output_path, station_filename):
        super().__init__(input_path, output_path, r"ID-(\d+)--(Met|Cal)_HH.csv")
        self.obs_regex = r"(\w+)(--H(\d*))?_([\w/|%]+)"
    
    def get_station_info(self):
        pass
    
    def process_df(self, df):
        pass
    
    def load_file(self, file):
        ddf = dd.read_csv(file, sep=",", decimal=".")
        return ddf