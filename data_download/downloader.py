import httpx
from abc import abstractmethod
from pathlib import Path
from datetime import datetime
from utils.utils import url_creator, get_timestamps
from functools import partial
from translate.translator import Translator
import json


class Downloader:
    def __init__(self, data_url, stations_url, raw_path, **kwargs):
        """
        Downloads data and turns it into intermediate format
        """
        self.raw_path = raw_path

        self.station_data = None
        self.data = None

        self.station_file = raw_path / "stations.json"
        self.data_file_format = "{site_id}-{year}-{month}.json"
        
        self.stations_url = stations_url
        # In format: https(...)/{year}/{month}/{day}/{hour}/{other_info}
        # order not important, and the other info must be provided in a json string
        self.data_url = data_url

        self.client = httpx.Client()
        self.other_data = kwargs.get("other_data", {})
        verbose = kwargs.get("verbose", False)
    
    # just get the stations data in {siteid: station_data} where station_data is a dict from json
    @abstractmethod
    def _get_stations_data(self):

        """
        Downloads the data for the given stations.
        """
        pass
    
    # for a list in [{url, info}] format transform into [{info, data}] | data in dictionary from json
    @abstractmethod
    def _get_data_for_station(self, urls):
        """
        Downloads the data for the given stations.
        """
        pass

    # Returns data in {siteid:[{info,data}]} format and stations in {siteid:station_data} format
    def _get_data(self,start,end):
        # stations_data: {siteid: ddf}
        stations = self._get_stations_data()
        timestamps = get_timestamps(start,end,time_interval="h")

        # data: {siteid: [{info, data}]}
        data = dict()
        # assumption: assuming that data will be available for each site separately and not altogether
        # might need to move this part to an abstract method
        for station in stations.keys():
            #station_urls [{urls, info}]
            station_urls = url_creator(self.data_url, timestamps, station, other_data=self.other_data)
            station_data = self._get_data_for_station(station_urls)
            data.update({station:station_data})

        return data, stations
    
    #saves the raw data to file
    def _save(self, data):
        for siteid, dictionary in data.items():
            filepath =  self.raw_path/ (self.data_file_format.format(**dictionary["info"]))
            with open(filepath, "w") as fp:
                json.dump(dictionary["data"], fp)          
    
    def download(self, start, end, save=False):
        """
        Downloads the data for the given stations.
        """
        data, stations = self._get_data(start, end)

        if save:
            self._save(data)
        
        return data, stations