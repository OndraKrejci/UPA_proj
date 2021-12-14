#!/usr/bin/env python3
##
# @file plot_graphs.py
# @author Oliver Kunik xkunik00@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 12/2021
# Plot graphs from CSV files

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
import scipy as sp
from matplotlib.dates import date2num
import datetime
#from tabulate import tabulate

def plot_A1():
    df = pd.read_csv('data_csv/A1-covid_po_mesicich.csv', delimiter=";")

    x = df['zacatek'].to_numpy()
    y = df[['nakazeni', 'vyleceni', 'hospitalizovani', 'testy']].to_numpy()

    plt.plot(x, y)
    plt.show()

def plot_A2():
    df = pd.read_csv('data_csv/A2-osoby_nakazeni_kraj.csv', delimiter=";")

    #replace NaN with Cudzinci
    df['kraj_nazev'].replace(np.nan, 'Cudzinci', regex=True, inplace=True)

    #drop outliers simple
    df.drop(df.loc[df['vek']>105].index, inplace=True)

    sns.boxplot(x="kraj_nazev", y="vek", data=df)
    plt.show()

def plot_B1():
    df = pd.read_csv('data_csv/B1-prirustky_kraj.csv', delimiter=";")
    df['datum_zacatek'] = pd.to_datetime(df['datum_zacatek'])
    mask = df['datum_zacatek'] == '2020-10-01'
    df = df.loc[mask]
    df['pomer'] = df['nakazeni_prirustek'] / df['kraj_populace']
    df.sort_values(by=['pomer'], inplace=True, ascending=False)
    ax = plt.subplot(111)
    ind = np.arange(df.shape[0])  # the x locations for the groups
    width = 0.2
    ax.bar(ind, df['kraj_populace'].to_numpy(), width=0.2, color='b', align='center')
    ax.bar(ind + width, df['nakazeni_prirustek'].to_numpy(), width=0.2, color='r', align='center')
    ax2 = ax.twinx()
    ax2.plot(df['kraj_nazev'].to_numpy(), df['pomer'].to_numpy())
    ax.set_xticklabels( df['kraj_nazev'].to_numpy() )
    plt.show()

def print_B1():
    df = pd.read_csv('data_csv/B1-prirustky_kraj.csv', delimiter=";")
    df['datum_zacatek'] = pd.to_datetime(df['datum_zacatek'])
    df['pomer'] = df['nakazeni_prirustek'] / df['kraj_populace']
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('display.precision', 5)

    dates = df["datum_zacatek"].unique()

    for x in dates:
        print(x)
        mask = df['datum_zacatek'] == x
        ndf = df[mask].copy()
        ndf.sort_values(by=['pomer'], inplace=True, ascending=False)
        ndf = ndf[['kraj_nazev', 'kraj_populace', 'nakazeni_prirustek','pomer']]
        ndf.index = np.arange(1, len(ndf)+1)
        print(ndf)




#plot_A1()
#plot_A2()
#plot_B1()
print_B1()

