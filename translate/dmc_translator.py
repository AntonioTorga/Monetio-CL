from translate.translator import Translator

class DMCTranslator(Translator):
    """
    DMC Translator class for translating DMC data to netcdf format.
    """
    def __init__(self, input_path, output_path, station_file_name):
        super().__init__(input_path, output_path, station_file_name, r"ID-(\d+)--(Met|Cal).csv")

    def raw_to_intermediate_file(self, files, filename, save=False):
        pass

    def raw_to_intermediate_station_file(self):
        pass