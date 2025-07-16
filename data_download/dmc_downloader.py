from .downloader import Downloader
from pathlib import Path
from utils.config import Config
import json
import asyncio
import httpx

async def fetch_all_data(urls):
    async with httpx.AsyncClient() as client:
        tasks = [client.get(url) for url in urls]
        responses = await asyncio.gather(*tasks)
        return [response.json() for response in responses]

        
class DMCDownloader(Downloader):
    def __init__(self, data_url, stations_url, raw_path, **kwargs):
        super().__init__(data_url, stations_url, raw_path, **kwargs)

    def _get_stations_data(self):
        response = self.client.get(self.stations_url.format(**self.other_data))
        # TODO: manage unsuccesful response
        station_data = response.json()
        station_data = station_data["datosEstacion"]
        station_data = {station["codigoNacional"]:station for station in station_data}
        return station_data
    
    # for a list in [{url, info}] format transform into [{info, data}] | data in dictionary from json
    def _get_data_for_station(self, station_urls):
        urls = [url["url"] for url in station_urls]
        info = [_info["info"] for _info in station_urls]

        responses = asyncio.run(fetch_all_data(urls))
        data = []
        for _info, _response in zip(info, responses):
            data.append({"info":_info, "data":_response})
        
        return data
        