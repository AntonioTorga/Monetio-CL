from data_download.dmc_downloader import DMCDownloader
from translate.dmc_translator import DMCTranslator


import typer
from rich import print
    
app = typer.Typer(name="Monetio-CL", help="Monetio Command Line Interface")

# TODO: connect this to the actual code
@app.command()
def get_sinca(start_date: str, end_date: str):
    """
    Get the SINCA data from the start_date to the end_date in Melodies-Monet format.
    """
    print("Downloading SINCA data...")
    return 0

@app.command()
def get_dmc(start_date: str, end_date: str, timestep: str):
    """
    Get the DMC data from the start_date to the end_date in Melodies-Monet format.
    """
    dmc_downloader = DMCDownloader()
    dmc_translator = DMCTranslator()

    dmc_downloader.download(start_date, end_date, timestep)
    dmc_translator.translate(start_date, end_date, timestep)


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