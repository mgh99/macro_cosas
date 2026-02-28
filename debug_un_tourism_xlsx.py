import pandas as pd

xlsx = "data/un_tourism_cache/UN_Tourism_8_9_1_TDGDP_04_2025/<TU_XLSX>.xlsx"
xl = pd.ExcelFile(xlsx)
print("Sheets:", xl.sheet_names)

for s in xl.sheet_names:
    df0 = pd.read_excel(xlsx, sheet_name=s, nrows=5, header=None)
    print("\n---", s, "---")
    print(df0)