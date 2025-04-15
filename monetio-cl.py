from download.downloader import Downloader
from translate.sincatranslator import SincaToNetCdf
from translate.dmctranslator import DmcToNetCdf

import typer
from rich import print
    
app = typer.Typer(name="Monetio-CL", help="Monetio Command Line Interface")

downloader = Downloader()

@app.command()
def get_sinca(start_date: str, end_date: str):
    """
    Get the SINCA data from the start_date to the end_date in Melodies-Monet format.
    """
    downloader.download_sinca(start_date, end_date)
    translator = sinca(

    )
    print("Downloading SINCA data...")
    pass

@app.command()
def get_dmc(start_date: str, end_date: str):
    """
    Get the DMC data from the start_date to the end_date in Melodies-Monet format.
    """
    downloader.download_dmc(start_date, end_date)

    # Implement the logic to get the DMC data
    pass

@app.command()  
def process_sinca(path):
    """
    Process the SINCA data from the given path into Melodies-Monet format.
    The path should point to the directory with the raw SINCA data files.
    """
    # Implement the logic to process the SINCA data
    pass

@app.command()
def process_dmc(path):
    """
    Process the DMC data from the given path into Melodies-Monet format.
    The path should point to the directory with the raw DMC data files.
    """
    # Implement the logic to process the DMC data
    pass

if __name__ == "__main__":
    app()