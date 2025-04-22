# class SincaToNetCdf(NetcdfTranslator):

#     def __init__(self, input_path, output_path, station_filename):
#         super().__init__(input_path, output_path, r"ID-(\d+)--(Met|Cal)_HH.csv")
#         self.obs_regex = r"(\w+)(--H(\d*))?_([\w/|%]+)"
    
from translate.translator import Translator

class SINCATranslator(Translator):
    """
    DMC Translator class for translating DMC data to netcdf format.
    """
    def __init__(self, input_path, output_path, station_filename):
        super().__init__(input_path, output_path, station_filename, r"ID-(\d+)--(Met|Cal)_HH.csv")
        self.obs_regex = r"(\w+)(--H(\d*))?_([\w/|%]+)"

    def 