import httpx
from abc import abstractmethod
from pathlib import Path
from datetime import datetime
from utils.utils import url_creator
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

        self.station_file = raw_path / "stations.json" if raw_path!=None else None
        self.data_file_format = "{siteid}-{year}-{month}.json"
        
        self.stations_url = stations_url
        # In format: https(...)/{year}/{month}/{day}/{hour}/{other_info}
        # order not important, and the other info must be provided in a json string
        self.data_url = data_url

        self.client = httpx.Client()
        self.other_data = kwargs.get("other_data", {})
        self.verbose = kwargs.get("verbose", False)
    
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

    @abstractmethod
    def _get_station_ids(self):
        pass 

    # Returns data in {siteid:[{info,data}]} format and stations in {siteid:station_data} format
    def _get_data(self, timestamps):
        # stations_data: {siteid: ddf}
        self.station_data = self._get_stations_data()
        
        # data: {siteid: [{info, data}]}
        data = dict()
        # assumption: assuming that data will be available for each site separately and not altogether
        # might need to move this part to an abstract method
        for station in self._get_station_ids():
            if self.verbose:
                print(f"Fetching data for station {station}...")
            #station_urls [{urls, info}]
            station_urls = url_creator(self.data_url, timestamps, station, other_data=self.other_data)
            station_data = self._get_data_for_station(station_urls)
            data.update({station:station_data})
    
        self.data = data

        return self.data, self.station_data
    
    #saves the raw data to file
    def _save(self):
        data = self.data
        for siteid, data_list in data.items():
            if self.verbose:
                print(f"Writting to file raw data from station {siteid}...")
            for dictionary in data_list:
                dictionary["info"].update({"siteid":siteid})
                filepath =  self.raw_path/ (self.data_file_format.format(**dictionary["info"]))
                with open(filepath, "w") as fp:
                    data_write = dictionary["data"] if dictionary["data"]!=None else {}
                    json.dump(dictionary["data"], fp)
        with open(self.station_file, "w") as fp:
            json.dump(self.station_data, fp)
    
    def download(self, timestamps=None, save=False):
        """
        Downloads the data for the given stations.
        """
        if timestamps==None:
            raise ValueError("No timestamps for download.")
        
        data, stations = self._get_data(timestamps)

        if save:
            self._save()
        
        return data, stations