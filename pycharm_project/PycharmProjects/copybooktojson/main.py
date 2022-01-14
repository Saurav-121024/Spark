import pandas as pd
from tkinter import *

df = pd.read_csv("C:/Users/saukumar/Desktop/AM00.txt", index_col=None, sep='\s+', header=0)

print(df)
dfjson = df.to_json(orient="records")

print(dfjson)