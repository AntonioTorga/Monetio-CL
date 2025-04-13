import pandas as pd
import xarray as xr
import pathlib as pt
import re

class Sinca2NetCdf: 
    def __init__(self, path_data: str, stations_filename: str):
        self.path = pt.Path(path_data).resolve()
        print(f"Path: {self.path}")
        self.path_stations = self.path / stations_filename
        print(f"Stations path: {self.path_stations}")
        self.df_data = None
        self.df_st = None

        self.heights = []
        self.latitudes = []
        self.longitudes = []
        self.filename_regex = r"ID-(\d+)--(Met|Cal)_HH.csv"
        self.obs_regex = r"(\w+)(--H(\d*))?_([\w/|%]+)"
        self.variables = {"Met":dict(), "Cal":dict()}

    def get_nums_from_string(self, string, sep=" "):
        """Get the numbers from a string"""
        nums = string.split(sep)
        nums = [float(n) for n in nums if n != ""]
        nums = list(set(nums))
        return nums
    
    def get_all_values_from_column(self, df ,column_name : str):
        lst = []
        if df[column_name].dtype=="float64":
            lst = list(set([float(x) for x in df[column_name].values]))
        elif df[column_name].dtype=="object":
            lst = self.get_nums_from_string(" ".join(df[column_name].values))
        lst.sort()
        return lst

    def load_stations(self):
        """Load the stations metadata"""
        self.df_st = pd.read_csv(self.path_stations, sep=",", decimal=".")

        self.heights = self.get_all_values_from_column(self.df_st, "Alturas")
        self.latitudes = self.get_all_values_from_column(self.df_st, "Latitud")
        self.longitudes = self.get_all_values_from_column(self.df_st, "Longitud")

        self.df_st = self.df_st.dropna(subset=["Latitud","Longitud","Elevacion"])[["ID-Stored", "Nombre",  "Latitud", "Longitud", "Elevacion"]]
        return self.df_st
        
    def load_data(self, filename: str):
        """Load the data from the file"""
        match = re.search(self.filename_regex, filename)
        if match:
            site_id, type_measurement = match.groups()
        df = pd.read_csv(self.path / filename, sep=",", decimal=".")

        # Get latitude and longitude from the stations dataframe
        df["latitude"] = self.df_st.loc[self.df_st["ID-Stored"] == int(site_id), "Latitud"].values[0]
        df["longitude"] = self.df_st.loc[self.df_st["ID-Stored"] == int(site_id), "Longitud"].values[0]

        print(df.columns)
        df.drop(["date_local", "date"], axis=1, inplace=True)
        print(df.columns)

        for col in df.columns:
            # Checks if the column is in the pattern <variable name>--H<height>_<unit>
            # renames if it is, add the variable info to a dict.
            match = re.search(self.obs_regex, col)
            if match:
                var_name, _, height, unit = match.groups()
                height = int(height) if height else None

                new_name = f"{var_name}--H{height}" if height else f"{var_name}"
                df.rename(columns={col: new_name}, inplace=True)
                if var_name not in self.variables[type_measurement]:
                    self.variables[type_measurement][var_name] = unit
        return df
    
    def load_mf_data(self):
        files = [f for f in pt.Path(self.path).iterdir() if f.is_file() and f.name.endswith(".csv")]
        dfs = {"Met":[], "Cal":[]}
        for f in files:
            # Check if the file name matches the regex
            match = re.search(self.filename_regex, f.name)
            if not match:
                print(f"File {f.name} does not match the pattern. Skipping.")
                continue
            site_id, type_measurement = match.groups()
            df = self.load_data(f.name)
            dfs.append(df)
        return dfs

    
    def run(self):
        self.load_stations()
        data = self.load_mf_data()
        # dfs = self.load_mf_data()
        # columns = set()
        # for df in dfs:
        #     columns.update(df.columns)
        # columns = sorted(columns)
        # # write columns to file
        # with open("columns.txt", "w") as f:
        #     for col in columns:
        #         f.write(f"{col}\n")

s2c= Sinca2NetCdf(path_data=".\\SINCA_20230720\\Data", stations_filename="Chile-Stations.csv")
s2c.run()
