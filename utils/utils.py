import numpy as np
import re


def to_float(x):
    try:
        return float(re.findall(r"[-+]?\d*\.?\d+", str(x))[0])
    except:
        print(f"Error converting {x} to float")
        return np.nan
