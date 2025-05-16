from translate.translator import Translator

class DMCTranslator(Translator):
    """
    DMC Translator class for translating DMC data to netcdf format.
    """
    def __init__(self, input_path, output_path):
        super().__init__(input_path, output_path)

    def raw_to_intermediate_file(self, files, filename=None, save=False):
        """
        Provided files are merged into a single dask dataframe.
        The resulting dataframe is saved to a csv file if save is True.
        """
        ddf = None
        #Swift through files
        for file in files:
            with open(file, "r") as f:
                data = json.load(f)
                data = data["datosEstaciones"]
                #Swift through stations

                for i in data:
                    id_estacion = i["estacion"]["codigoNacional"]
                    if i["datos"] == [] or i["datos"] is None:
                        print(f"Estación {id_estacion} no tiene datos en el archivo {file.name}.")
                        continue
                    columns = list(i["datos"][0].keys())
                    columns = {k: [] for k in columns}
                    columns["id_estacion"] = id_estacion
                    #Swift through data points ID(time, site_id)
                    for j in i["datos"]:
                        #Swift through data fields for a given data point
                        for k,v in j.items():
                            columns[k].append(v)
                    _ddf = dd.from_dict(columns, npartitions=10)
                    if ddf == None:
                        ddf = _ddf
                    else: 
                        ddf = ddf.merge(_ddf, how="outer")
        if save and filename!=None:
            ddf.to_csv(self.output_path / (filename+".csv"), index=False, single_file=True, header=True)
        
        return ddf

    def raw_to_intermediate_station_file(self, save=False):
        """
        Converts the raw station data (.json) to an intermediate station file (.csv).
        """
        filename = self.input_path / (self.station_file_name+".json")

        if filename.exists():
            with open(filename, "r") as file:
                data = json.load(file)
                data = data["datosEstacion"]
                columns = list(data[0].keys())
                columns = {k: [] for k in columns}
            
                for station in data:
                    for k,v in station.items():
                        columns[k].append(v)

                ddf = dd.from_dict(columns, npartitions=10)
                if save:
                    ddf.to_csv(self.output_path / (self.station_file_name+".csv"), index=False, single_file=True, header=True)
        
                return ddf