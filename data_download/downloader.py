import httpx
from abc import abstractmethod
from pathlib import Path
from datetime import datetime
from dateutil import parser
from utils.config import Config

class Downloader:
    def __init__(self, start_timestamp, end_timestamp, time_interval, config : Config, raw_data_path = None, intermediate_data_path = None):
        """
        Downloader class to download data from the API.
        """
        self.config = config
        self.data_type = None
        
        self.raw_data_path = raw_data_path if raw_data_path else Path.cwd() / self.config.defaults["raw"]["path"]

        if self.raw_data_path and not self.raw_data_path.is_absolute():
            self.raw_data_path = Path.cwd() / self.raw_data_path
            
        self.raw_data_path.mkdir(parents=True, exist_ok=True)

        self.data_fn_format = "{0}-{1}-{2}-{3}.json" # station, type, year, month
        self.station_fn = "stations.json"

        parser_info= parser.parserinfo(dayfirst=True)
        self.start_dt = parser.parse(start_timestamp, parserinfo=parser_info)
        self.end_dt = parser.parse(end_timestamp, parserinfo=parser_info)
        if self.start_dt > self.end_dt:
            raise ValueError("Start date must be before end date")
        
        self.time_interval = time_interval

        self.timestamps = None
        self.station_ids = []

        self.stations_url = None
        self.data_url = None
                          
        self.client = httpx.Client()

    def get_timestamps(self):
        """
        Returns the timestamps for every moment in the time interval
        with the right timestep and format
        """

        import pandas as pd
        timestamps = pd.date_range(start=self.start_dt, end=self.end_dt, freq=self.time_interval).to_list()
        timestamps = [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in timestamps]
        self.timestamps = timestamps

    def write_to_file(self, data, filename):
        """
        Writes the data to a file.
        """
        with open(self.raw_data_path / filename, "w") as file:
            file.write(data)

    @abstractmethod
    def _get_stations_data(self):

        """
        Downloads the data for the given stations.
        """
        pass

    @abstractmethod
    def _get_data(self):
        """
        Downloads the data for the given stations.
        """
        pass

    def download(self):
        """
        Downloads the data for the given stations.
        """
        self.get_timestamps()

        stations_data = self._get_stations_data()
        data = self._get_data()