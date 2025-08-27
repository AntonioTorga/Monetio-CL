from translate.dmc_translator import DMCTranslator
from translate.translator import Translator
from data_download.dmc_downloader import DMCDownloader
from utils.utils import (
    check_file,
    check_path_exists,
    to_datetime,
    get_timestamps,
    get_existing_timestamps,
)
from enum import StrEnum
from datetime import datetime

import typer
from typing_extensions import Annotated
from pathlib import Path
from rich import print


class Timestep(StrEnum):
    """Enum for the timestep options."""

    H = "H"  # Hourly
    h = "h"  # hourly
    D = "D"  # Daily
    M = "M"  # Monthly
    Y = "Y"  # Yearly
    N = "N"


app = typer.Typer(
    name="Monetio-CL",
    help="Monetio Command Line Interface",
    pretty_exceptions_enable=False,
)


@app.command()
def get_dmc(
    start_time: Annotated[datetime, typer.Argument(formats=["%Y-%m-%d", "%d-%m-%Y"])],
    end_time: Annotated[datetime, typer.Argument(formats=["%Y-%m-%d", "%d-%m-%Y"])],
    user: Annotated[str, typer.Argument()],
    api_key: Annotated[str, typer.Argument()],
    # raw_path: Annotated[Path, typer.Option(exists=True, dir_okay=True, file_okay=False, resolve_path=True)] = Path("./raw_data"),
    intermediate_path: Annotated[
        Path,
        typer.Option(exists=True, dir_okay=True, file_okay=False, resolve_path=True),
    ] = Path("./inter_data"),
    output_path: Annotated[
        Path,
        typer.Option(exists=True, dir_okay=True, file_okay=False, resolve_path=True),
    ] = Path("./output_data"),
    output_name: str = typer.Option(r"dmc.nc", "--output-name", "-o"),
    timestep: str = typer.Option(Timestep.N, "--timestep", "-t"),
    location_attr_names: str = typer.Option(None, "--location-attribute-names", "-l"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    cached: bool = typer.Option(False, "--cached", "-c"),
    save_intermediate: bool = typer.Option(False, "--save-intermediate"),
):
    """
    Download DMC data from start_time to end_time, and process into Melodies-Monet netcdf format.
    If wanted saves the "intermediate" tabulated data in .csv format.
    Also able to merge downloaded data with existing data in intermediate_path
    """

    timestamps = get_timestamps(start_time, end_time, time_interval=timestep)

    if intermediate_path != None and cached:
        existing_timestamps = get_existing_timestamps(
            intermediate_path, r"{\d}.csv", time_name="momento"
        )
        timestamps = list(set(timestamps) - set(existing_timestamps))

    downloader = DMCDownloader(
        other_data={"user": user, "api_key": api_key}, verbose=verbose
    )
    translator = DMCTranslator(
        intermediate_path,
        output_path,
        verbose=verbose,
        output_name=output_name,
        timestep=timestep,
    )

    location_attr_names = (
        location_attr_names.split(",") if location_attr_names != None else []
    )

    print(
        f"Downloading DMC data from {start_time.strftime("%d-%m-%Y")} to {end_time.strftime("%d-%m-%Y")}"
    )
    data, station = downloader.download(timestamps)
    translator.from_raw_to_netcdf(
        data,
        station,
        time_name="momento",
        lat_name="latitud",
        lon_name="longitud",
        id_name="codigoNacional",
        location_attr_names=location_attr_names,
        save=save_intermediate,
        start=start_time,
        end=end_time,
        time_interval=timestep,
        merge=cached,
    )

    return data, station


@app.command()
def process_intermediate_data(
    intermediate_path: str = typer.Option(
        r".\intermediate_data", "--intermediate-path", "-i"
    ),
    station_file: str = typer.Option("station.csv", "--station-filename", "-s"),
    filename_regex: str = typer.Option(r"(\d+).csv", "--filename-regex", "-r"),
    output_path: str = typer.Option(r".\MM_data", "--output-path"),
    output_name: str = typer.Option(r"data_in_nc.nc", "--output-name"),
    lat_name: str = typer.Option("Latitud", "--lat-name"),
    lon_name: str = typer.Option("Longitud", "--lon-name"),
    time_name: str = typer.Option("time", "--time-name"),
    location_attr_names: str = typer.Option(None, "--location-attr-names", "-l"),
    id_name: str = typer.Option("ID-Stored", "--id-name"),
    timestep: str = typer.Option(Timestep.N, "--timestep", "-t"),
    verbose: bool = typer.Option(False, "--verbosity", "-v"),
):
    """
    Process a batch of data located in intermediate_path, that is in intermediate format.
    Intermediate format is caracterized by using wide format in tabular data, where each file
    contains the data of 1 (one) station. It can have any number of data variables.
    """

    intermediate_path = Path(intermediate_path)
    station_file = (
        Path(station_file)
        if Path(station_file).exists()
        else intermediate_path / station_file
    )

    check_path_exists(intermediate_path)
    check_path_exists(output_path, create=True)
    check_file(station_file)
    location_attr_names = (
        location_attr_names.split(",") if location_attr_names != None else []
    )

    translator = Translator(
        intermediate_path,
        output_path,
        raw_path=None,
        verbose=verbose,
        intermediate_filename_regex=filename_regex,
        station_filename=station_file,
        output_name=output_name,
        timestep=timestep,
    )

    translator.from_intermediate_to_netcdf(
        time_name, id_name, lat_name, lon_name, location_attr_names
    )


if __name__ == "__main__":
    app()
