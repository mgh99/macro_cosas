import pandas as pd

path = "data/un_tourism_xlsx/UN_Tourism_inbound_arrivals_by_region_12_2025.xlsx"
xl = pd.ExcelFile(path)
print("SHEETS:", xl.sheet_names)

df = pd.read_excel(path, sheet_name=xl.sheet_names[0], header=None, nrows=30)
print(df)