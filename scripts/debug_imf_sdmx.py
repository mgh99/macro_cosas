import sdmx

IMF = sdmx.Client("IMF_DATA")
msg = IMF.data("CPI", key="USA.CPI.CP01.IX.M", params={"startPeriod": 2018})
s = sdmx.to_pandas(msg)
print(s.head())