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



plot_A1()
plot_A2()
plot_B1()

