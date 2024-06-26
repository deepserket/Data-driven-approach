# -*- coding: utf-8 -*-

from glob import glob
from functools import lru_cache
from collections import Counter, defaultdict

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


def stats_over_time():
    # displays basic stats of the warehouse for each DB export in the current directory
    do_not_read = ("AXXXXXXX", "DC_ONDEMAND", "COOP0001", "COLLH001", "COLLAUDO", "COOP0000", "DEPFILTR")
    for file in glob("giacenze*.csv"):
        if "dettagli" in file: continue
        print(f"Analyzing {file}:")
        
        tot, cass, dep = stats(file)
        
        print("scaff: ", tot, end=" |\t")
        print(f"cassette e colli: {cass} |  depositi: {dep}")
        print("================================================================================================")

@lru_cache # lru_cache returns the same object, do not modify in place the result!
def stats(file):
    # return basic stats 
    do_not_read = ("AXXXXXXX", "DC_ONDEMAND", "COOP0001", "COLLH001", "COLLAUDO", "COOP0000", "DEPFILTR")
    df = read_and_clean(file)    
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
    return tot, cass, dep


def plot_stats_over_time():
    dates = list()
    values = list()
    for file in glob("giacenze*.csv"):
        if "dettagli" in file: continue
        # name format: giacenzeYYYYMMDD-HHMM.csv    (-HHMM is optional)
        dates.append(file[10:-4]) # TODO better parsing
        t, c, d = stats(file)
        values.append([*list(t.values()), c, d])
    fig, ax = plt.subplots()
    labels = "A - 900k", "C", "D", "Refill", "Depositi"
    for v in values:
        v[0] -= 900_000 
    for i, l in enumerate(labels):
        ax.plot(dates, [v[i] for v in values], label=l)
    #ax.set_yscale('log') # is log good enough? maybe a % of products above 0 or above qta_riordino might be better
    ax.legend()
    ax.tick_params(axis='x', labelrotation=45)
    plt.show()

def plot_change_over_time():
    # this feels kinda bad, i'll probably remove it
    dates = list()
    values = list()
    prev_tA, prev_tC, prev_tD = 0, 0, 0
    prev_c, prev_d = 0, 0
    first = True
    for file in glob("giacenze*.csv"):
        if "dettagli" in file: continue
        # name format: giacenzeYYYYMMDD-HHMM.csv    (-HHMM is optional)
        dates.append(file[10:-4]) # TODO better parsing
        t, c, d = stats(file)
        if first:
            first = False
            prev_tA = t["A"]
            prev_tC = t["C"]
            prev_tD = t["D"]
            prev_c = c
            prev_d = d
        values.append([t["A"] - prev_tA,
                       t["C"] - prev_tC, 
                       t["D"] - prev_tD,
                       c - prev_c,
                       d - prev_d])
        prev_tA = t["A"]
        prev_tC = t["C"]
        prev_tD = t["D"]
        prev_c = c
        prev_d = d
    fig, ax = plt.subplots()
    labels = "A", "C", "D", "Refill", "Depositi"
    for i, l in enumerate(labels):
        ax.plot(dates, [v[i] for v in values], label=l)
    ax.set_yscale('log') # is log good enough? maybe a % of products above 0 or above qta_riordino might be better
    ax.legend()
    plt.show()
    

def read_and_clean(file):
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
    return df


# TODO
#   Data cleaning
#   Automatic DB export
#   Cool Graphs (OwO)
#   Priorities
#   Search for UDCs and UDMs for fast refill (With FIFO?)
#   Managing mixed UDCs
#   etc...
