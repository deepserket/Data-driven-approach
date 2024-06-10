# -*- coding: utf-8 -*-

from glob import glob
from collections import Counter, defaultdict

import numpy as np
import pandas as pd

def stats_over_time():
    # displays basic stats of the warehouse for each DB export in the current directory
    do_not_read = ("AXXXXXXX", "DC_ONDEMAND", "COOP0001", "COLLH001", "COLLAUDO", "COOP0000", "DEPFILTR")
    for file in glob("giacenze*.csv"):
        if "dettagli" in file: continue
        print(f"Analyzing {file}:")
        df = pd.read_csv(file, on_bad_lines="warn", sep=";", low_memory=False,
                         usecols=['Udc', 'Magazzino', 'Vano', 'Udm', 'Articolo',
                                  'Descrizione articolo', 'Commessa', 'Quantità', 
                                  'Q.tà disp.', 'Q.tà riordino', 'Q.tà max', 
                                  'Cat. Merceologica'],  
                         dtype="string")

        df = df.rename(columns = {'Q.tà disp.': "dispo",
                                  'Q.tà riordino': "riordino", 
                                  'Q.tà max': "Qmax", 
                                  'Cat. Merceologica': "categoria",
                                  'Descrizione articolo': 'Descrizione'}) #TODO standardization of cases
        
        if df.Udc[0] is pd.NA: # remove "Total" row
            df = df.iloc[1:, :]
        else:
            df = df.iloc[:-1, :]
        
        df = df.drop(df[df.Vano == "E0010101"].index) #280k lines, mostly rubbish, TODO
        
        zone = ("A", "C", "D")
        tot = {"A": 0,
               "C": 0,
               "D": 0}
        cass = 0
        dep = 0

        for index, row in df.iterrows():
            if row.Vano in do_not_read: continue
            if row.Descrizione.startswith("SEMIL"): continue # what is this product?
            dispo = int(row.dispo)
            if row.Vano == "P0100000" or row.Vano == "R0000000":
                cass += dispo
            if row.Vano[0] in zone:
                if row.Vano[-2:] == "00":
                    dep += dispo
                else:
                    tot[row.Vano[0]] += dispo
        print("scaff: ", tot, end=" |\t")
        print(f"cassette e colli: {cass} |  depositi: {dep}")
        print("================================================================================================")

# TODO
#   Data cleaning
#   Automatic DB export
#   Cool Graphs (OwO)
#   Priorities
#   Search for UDCs and UDMs for fast refill (With FIFO?)
#   Managing mixed UDCs
#   etc...
