from data_download.dmc_downloader import DMCDownloader
from translate.dmc_translator import DMCTranslator
from enum import StrEnum

import typer
from rich import print

class Timestep(StrEnum):
    """Enum for the timestep options."""
    H = "H"  # Hourly
    D = "D"  # Daily
    M = "M"  # Monthly
    Y = "Y"  # Yearly
    
app = typer.Typer(name="Monetio-CL", help="Monetio Command Line Interface")

# TODO: connect this to the actual code
@app.command()
def get_sinca(start_date: str, end_date: str, timestep: str = typer.Option(Timestep.H, "--timestep", "-t"),
            raw_data_path: str = typer.Option("/raw_data", "--raw-data-path", "-r"),
            intermediate_path: str = typer.Option("/inter_data", "--intermediate-path", "-i"),
            output_path: str = typer.Option("/MM_data", "--output-path", "-o")):
    """
    Get the SINCA data from the start_date to the end_date in Melodies-Monet format.
    """
    print("Downloading SINCA data...")
    return 0

@app.command()
def get_dmc(start_date: str, end_date: str, timestep: str = typer.Option(Timestep.H, "--timestep", "-t"), 
            email: str = typer.Option(..., prompt=True),
            apiKey: str = typer.Option(..., prompt=True, hide_input=True),
            raw_data_path: str = typer.Option("/raw_data", "--raw-data-path", "-r"),
            intermediate_path: str = typer.Option("/inter_data", "--intermediate-path", "-i"),
            output_path: str = typer.Option("/MM_data", "--output-path", "-o"), 
            save_intermediate: bool = typer.Option(False, "--save-intermediate", "-s")):
    """
    Get the DMC data from the start_date to the end_date in Melodies-Monet format.
    """
    dmc_downloader = DMCDownloader(start_date, end_date, timestep, raw_data_path, mail=email, api_key=apiKey)
    dmc_translator = DMCTranslator(input_path=intermediate_path, output_path=output_path)

    dmc_downloader.download()
    dmc_translator.from_raw_to_netcdf(save=save_intermediate)


    # Implement the logic to get the DMC data
    pass

@app.command()  
def process_sinca(intermediate_path: str = typer.Option("/inter_data", "--intermediate-path", "-i"),
                  output_path: str = typer.Option("/MM_data", "--output-path", "-o")):
    """
    Process the SINCA data from the given path into Melodies-Monet format.
    The path should point to the directory with the raw SINCA data files.
    """
    # Implement the logic to process the SINCA data
    pass

@app.command()
def process_dmc(intermediate_path: str = typer.Option("/inter_data", "--intermediate-path", "-i"),
                output_path: str = typer.Option("/MM_data", "--output-path", "-o")):
    """
    Process the DMC data from the given path into Melodies-Monet format.
    The path should point to the directory with the raw DMC data files.
    """
    dmc_translator = DMCTranslator()
    
    pass

if __name__ == "__main__":
    app()