# -*- coding: utf-8 -*-

""" Outputs a prediction of the UDCs that would be ordered for a given article
or picking list """

import pandas as pd

def sortable_date(date):
    # returns a sortable (dictionary order == tempoal order) date
    # original format:  DD/MM/YYYY HH:MM:SS
    # output format: YYYY/MM/DD HH:MM:SS
    date, time = date.split()
    d, m, y = date.split("/")
    return f"{y}/{m}/{d} {time}"

def read_dettagli(file):
    # should be given the latest giacenzadettagli.csv available for better results
    df = pd.read_csv(file, on_bad_lines="warn", sep=";", low_memory=False,
                     usecols=['Udc', 'Magazzino', 'Vano', 'Udm', 'Articolo',
                              'Descrizione art.', 'Commessa', 'Quantità', 
                               'Data ingresso', 'Fustella',
                              'Cat. merceologica'],  
                     dtype="string")

    df = df.rename(columns = {'Data ingresso': "Data",
                              'Cat. merceologica': "Categoria",
                              'Descrizione art.': 'Descrizione'})
    df = df.astype({"Quantità": "int"})
    
    if df.Udc[0] is pd.NA: # remove "Total" row
        df = df.iloc[1:, :]
    else:
        df = df.iloc[:-1, :]
    
    df["Data"] = df["Data"].apply(sortable_date)
    df = df.sort_values("Data")
    
    return df

def predict(df, art, qty=1000):
    # generator that yields one UDC at a time until qty is satisfied or there are no more UDCs to order
    prev_Udc = None
    qty_Udc = 0
    for index, row in (df[df.Articolo == art]).iterrows():
        if row.Commessa != " ": continue # row with commessa are not counted
        if row.Vano.startswith("S") or row.Vano == "P0000000": # are UDCs in X_CLIENTE able to be requested?
            if prev_Udc is None:
                prev_Udc = row.Udc
                qty_Udc += row.Quantità
            elif prev_Udc == row.Udc:
                qty_Udc += row.Quantità
            else:
                yield prev_Udc, qty_Udc
                qty -= qty_Udc
                prev_Udc = row.Udc
                qty_Udc = row.Quantità
            
            if qty <= 0:
                return
    yield "rimanenze: ", qty
    return
        