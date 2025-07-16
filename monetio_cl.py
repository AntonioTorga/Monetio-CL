from translate.translator import Translator
from data_download.downloader import Downloader
from utils.utils import check_file, check_path_exists 
from enum import StrEnum

import typer
from pathlib import Path
from rich import print

class Timestep(StrEnum):
    """Enum for the timestep options."""
    H = "H"  # Hourly
    h = "h"  #hourly
    D = "D"  # Daily
    M = "M"  # Monthly
    Y = "Y"  # Yearly
    N = "N"
    
app = typer.Typer(name="Monetio-CL", help="Monetio Command Line Interface")

@app.command()
def get_dmc(
            start_time: str,
            end_time: str,
            user: str,
            api_key: str,
            timestep: str = typer.Option(Timestep.N, "--timestep", "-t"),
            output_path: str = typer.Option(r".\MM_data", "--output-path"),
            station_file:str = typer.Option("stations.csv", "--station-filename", "-s"),
            intermediate_filename:str = typer.Option(r"{siteid}.csv", "--intermediate-filename"),
            intermediate_regex:str = typer.Option(r"(\d+).csv", "--filename-regex"),
            output_name: str = typer.Option(r"dmc.nc", "--output-name", "-o"),
            verbose: bool=typer.Option(False, "--verbosity", "-v"),
            save_intermediate: bool =typer.Option(False, "--save_intermediate"),
            intermediate_path:str = typer.Option(None, "--intermediate-path", "-i"),
            ):
    output_path = Path(output_path)
    check_path_exists(output_path, create=True)
    if save_intermediate and intermediate_path:
        intermediate_path = Path(intermediate_path)
        check_path_exists(intermediate_path, create=True)

    data_url = r"https://climatologia.meteochile.gob.cl/application/servicios/getDatosRecientesEma/{siteid}/{year}/{month}?usuario={user}&token={api_key}"
    stations_url = r"https://climatologia.meteochile.gob.cl/application/servicios/getEstacionesRedEma?usuario={user}&token={api_key}"
    
    #TODO: confusion con donde poner que, es necesario darle tanta libertad al usuario?
    downloader = Downloader(data_url, stations_url, intermediate_path = intermediate_path, other_data={"user":user, "api_key":api_key})
    translator = Translator(intermediate_path, output_path, verbose=verbose, intermediate_regex = intermediate_regex , station_filename = station_file, output_name = output_name, timestep = timestep)

    data, station = downloader.download(start_time, end_time, save_intermediate=True)

    
@app.command()
def process_intermediate_data(intermediate_path:str = typer.Option(r".\intermediate_data", "--intermediate-path", "-i"),
                              station_file:str = typer.Option("station.csv", "--station-filename", "-s"),
                              filename_regex:str = typer.Option(r"(\d+).csv", "--filename-regex", "-r"),
                              output_path: str = typer.Option(r".\MM_data", "--output-path"),
                              output_name: str= typer.Option(r"data_in_nc.nc", "--output-name"),
                              lat_name: str = typer.Option("Latitud", "--lat-name"),
                              lon_name: str = typer.Option("Longitud", "--lon-name"),
                              time_name: str= typer.Option("timestamp", "--time-name"),
                              id_name: str= typer.Option("ID-Stored", "--id-name"),
                              timestep: str = typer.Option(Timestep.N, "--timestep", "-t"),
                              verbose: bool=typer.Option(False, "--verbosity", "-v")
                              ):
    """
    Process a batch of data located in intermediate_path, that is in intermediate format.
    Intermediate format is caracterized by using wide format in tabular data, where each file
    contains the data of 1 (one) station. It can have any number of data variables.
    """
    
    intermediate_path = Path(intermediate_path)
    station_file = Path(station_file) if Path(station_file).exists() else intermediate_path/station_file

    check_path_exists(intermediate_path)
    check_path_exists(output_path, create=True)
    check_file(station_file)

    translator = Translator(intermediate_path, output_path, verbose=verbose, intermediate_filename_regex = filename_regex, station_filename = station_file, output_name = output_name, timestep = timestep)

    translator.from_intermediate_to_netcdf(time_name, id_name, lat_name, lon_name)
    
if __name__ == "__main__":
    app()