from .downloader import Downloader
from pathlib import Path
from utils.config import Config

class DMCDownloader(Downloader):
    """
    Class to download data from the DMC API.
    """

    def __init__(self, start_timestamp, end_timestamp, raw_data_path, time_interval, config : Config ,  mail=None, api_key=None, output_path=None):
        super().__init__(start_timestamp, end_timestamp, raw_data_path, time_interval)
        self.config = config
        self.config.observation = "dmc"

        self.user["mail"] = mail
        self.user["api_key"] = api_key

        self.data_type = "Met"

        self.stations_url = self.config.get_station_url()
        self.data_url = self.config.get_data_url()
        

        self.output_path = Path(output_path) if output_path else Path.cwd() / self.config.defaults["output"]["path"]
        self.output_path.mkdir(parents=True, exist_ok=True)

    def filter_by_timestamps_required(self, data):
        """
        Filters the data by the timestamps required.
        """
        filtered_data = []
        # TODO: separate the getting of data because it could be data["..."]["..."] or data["..."][0]["..."]
        # and one of those shouldn't work.
        for data_dict in data["datosEstaciones"]["datos"]:
            if data_dict["momento"] in self.timestamps:
                filtered_data.append(data_dict)
        data["datosEstaciones"]["datos"] = filtered_data
        return data
    
    def get_monthly_timestamps(self):
        """
        Returns the timestamps for every moment in the time interval
        with the right timestamp.
        """
        import pandas as pd
        timestamps = pd.date_range(start=self.start_dt, end=self.end_dt, freq="d").to_list()
        timestamps = [dt.strftime("%Y-%m") for dt in timestamps]
        
        monthly_timestamps = []
        for timestamp in timestamps:
            year, month = timestamp.split("-")
            monthly_timestamps.append((year,month))
        return monthly_timestamps

    def _get_stations_data(self):
        """
        Downloads the data for the given stations.
        """
        response = self.client.get(self.stations_url.format(self.user["mail"], self.user["api_key"]))
        data = response.json()
        self.write_to_file(data, self.station_fn)

        self.station_ids = [station["codigoNacional"] for station in data["datosEstacion"]]
        
    def _get_data(self):
        """
        Downloads the data for the given stations using Async
        """
        monthly_timestamps = self.get_monthly_timestamps()

        import httpx
        import asyncio

        async def fetch_data(client, url, station_id, year, month):
            response = await client.get(url)
            if response.status_code != 200:
                raise Exception(f"Error fetching data: {response.status_code}")
            
            data = self.filter_by_timestamps_required(response.json())
            self.write_to_file(data, self.data_fn_format.format(station_id, self.data_type, year, month))

        async def main():
            async with httpx.AsyncClient() as client:
                tasks = []
                for station_id in self.station_ids:
                    for year, month in monthly_timestamps:
                        url = self.data_url.format(station_id, year, month, self.user["mail"], self.user["api_key"])
                        tasks.append(asyncio.create_task(fetch_data(client, url, station_id, year, month)))

                results = await asyncio.gather(*tasks)
                return results
        
        print("Downloading data...")
        asyncio.run(main())
        