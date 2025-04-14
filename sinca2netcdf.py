import dask.dataframe as dd
import pandas as pd
import xarray as xr
import pathlib as pt
import re

        # self.filename_regex = r"ID-(\d+)--(Met|Cal)_HH.csv"
        # self.obs_regex = r"(\w+)(--H(\d*))?_([\w/|%]+)"

class Sinca2NetCdf:
    def __init__(self):
        self.filename_regex = r"ID-(\d+)--(Met|Cal)_HH.csv"
        self.obs_regex = r"(\w+)(--H(\d*))?_([\w/|%]+)"
        self.in_path = pt.Path("data/")
        self.out_path = pt.Path("output/")
        self.out_path.mkdir(parents=True, exist_ok=True)

    def convertion(self):
        read_files()

