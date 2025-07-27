from translate.translator import Translator
from dask import dataframe as dd
import pandas as pd
import json

class DMCTranslator(Translator):
    def __init__(self, intermediate_path, output_path, **kwargs):
        super().__init__(intermediate_path, output_path, **kwargs)

    def _to_pd_dataframe(self, data):
        _data = data["data"]
        if _data:
            _data = _data["datosEstaciones"]["datos"]
            return pd.DataFrame(_data)
        return pd.DataFrame()

    def raw_to_intermediate_station(self, station_data):
        station_ddf = dd.from_pandas(pd.DataFrame(station_data["datosEstacion"]))
        return station_ddf
