import numpy as np
import re
from pathlib import Path


def to_float(x):
    try:
        return float(re.findall(r"[-+]?\d*\.?\d+", str(x))[0])
    except:
        return np.nan

def url_creator(url, timestamps, siteid, other_data={}):
    """
    Function to create url's from a template and a dictionary of parameters.
    """
    _other_data= other_data
    _other_data.update({"siteid":siteid})
    _format_dicts = [{"year":timestamp.year, "day":timestamp.day, "hour":timestamp.hour, "minute":timestamp.minute} for timestamp in timestamps]
    _format_dicts = [_format_dicts.update(_other_data)]
    
    urls = []
    for format_dict in _format_dicts:
        _url = url.format(**format_dict)
        dictionary = {"url":_url, "info":format_dict}
        urls.append(dictionary)

    return urls

def check_file(file_path):
    if not (file_path.exists() and  file_path.is_file()):
        raise FileNotFoundError(f"Couldn't open file {file_path}")

def check_path_exists(path, create=False):
    if path and not path.is_absolute():
        path = path.resolve()
    if create:
        try:
            path.mkdir(parents=True, exist_ok=True)
        except:
            raise OSError(f"Couldn't create path {path}")
    else:
        raise FileNotFoundError(f"Couldn't reach path {path}")

def get_timestamps(start, end, time_interval):
    """
    Returns the timestamps for every moment in the time interval
    with the right timestep and format
    """
    import pandas as pd
    from dateutil import parser

    parser_info= parser.parserinfo(dayfirst=True)
    start_dt = parser.parse(start, parserinfo=parser_info)
    end_dt = parser.parse(end, parserinfo=parser_info)

    if start_dt > end_dt:
        raise ValueError("Start date must be before end date")

    if time_interval=="N" or time_interval=="H":
        time_interval = "h"

    timestamps = pd.date_range(start=start_dt, end=end_dt, freq=time_interval).to_list()
    timestamps = [dt.strftime("%Y-%m-%d %H:%M:%S") for dt in timestamps]

    return timestamps

def create_data_vars_dict(data_df, timestamps, columns):
    data_vars = {}
    for col in columns:
        data_vars[col] =  (["x","time"], [])
        for i, df in enumerate(data_df):
            if col in df.columns.to_list():
                data_vars[col][1].append(df[col].to_numpy())
            else: 
                data_vars[col][1].append(np.full(len(timestamps), np.nan))
    return data_vars