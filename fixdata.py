import pandas as pd

flights = pd.read_csv("flights.csv", keep_default_na=False)

flights["DEP_DEL15"] = pd.to_numeric(flights["DEP_DEL15"], errors="coerce").fillna(0).astype(int).map({1: True, 0: False})

print(flights["DEP_DEL15"].value_counts())