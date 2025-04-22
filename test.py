import requests
from datetime import datetime

def upload_file(estacion, par, ini, fin, typee, timeframe):
    ## Dates asked for
    ini=ini.strftime("%y%m%d%H")
    fin=fin.strftime("%y%m%d%H")

    ## Embed type of variable to download onto string
    index=par.find('#')-2
    thirdline=par[:index] + typee + par[index:]

    ## Merge all parts into single file
    string='CR2\nlgudch\nCONAMA\n%s%sM%s\n%s\n%s\nEOF'%(estacion.strip(), timeframe, thirdline, ini, fin)

    return string


up_file = upload_file("1", "PM25  # [ug/m3]",  datetime(1990, 1, 1, 0), datetime.now(), "VAL", "HH")
print(up_file)
upload_url = "http://sinca.mma.gob.cl/cgi-bin/iairviro/tsexport.cgi"

data = requests.get(upload_url, data=up_file)
print(data.text)