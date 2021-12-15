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
import matplotlib.ticker as mtick
from sklearn import preprocessing
#from tabulate import tabulate
from matplotlib.ticker import StrMethodFormatter, NullFormatter
import matplotlib.dates as mdates
from matplotlib.ticker import AutoMinorLocator
import os
from matplotlib.pyplot import figure

def plot_A1():
    df = pd.read_csv('data_csv/A1-covid_po_mesicich.csv', delimiter=";")

    df['zacatek'] = pd.to_datetime(df['zacatek'])

    fig = plt.figure(figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)

    ax.plot(df['zacatek'].to_numpy(), df['nakazeni'].to_numpy(), label='Nakazení')
    ax.plot(df['zacatek'].to_numpy(), df['vyleceni'].to_numpy(), label='Vyliečení')
    ax.plot(df['zacatek'].to_numpy(), df['hospitalizovani'].to_numpy(), label='Hospitalizovaní')
    ax.plot(df['zacatek'].to_numpy(), df['testy'].to_numpy(), label='Testy')

    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(StrMethodFormatter('{x:.0f}'))

    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b \'%y'))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter('%b'))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))
    ax.yaxis.set_minor_locator(mtick.LogLocator(base=10.0, numticks=6))
    for label in ax.get_xticklabels(which='both'):
        label.set(rotation=30, horizontalalignment='right')

    ax.set_ylim(100, 2500000)
    ax.set_xlim(df['zacatek'].iloc[0], df['zacatek'].iloc[-1])

    plt.ylabel('Počet')
    fig.suptitle('Dotaz A1', fontsize=20)
    ax.legend()
    plt.subplots_adjust( left=0.15, right=0.98)

    plt.savefig("A1.svg", dpi=300)

    #plt.show()

def plot_A2():
    df = pd.read_csv('data_csv/A2-osoby_nakazeni_kraj.csv', delimiter=";")

    #replace NaN with Cudzinci
    df['kraj_nazev'].replace(np.nan, 'Cudzinci', regex=True, inplace=True)

    #drop outliers
    Q1 = df['vek'].quantile(0.25)
    Q3 = df['vek'].quantile(0.75)
    IQR = Q3 - Q1

    df = df[~((df['vek'] < (Q1 - 1.5 * IQR)) |(df['vek'] > (Q3 + 1.5 * IQR)))]

    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)

    ax = sns.boxplot(ax=ax, x="kraj_nazev", y="vek", data=df)
    for label in ax.get_xticklabels(which='both'):
        label.set(rotation=30, horizontalalignment='right')

    ax.set_title('Dotaz A2')
    plt.subplots_adjust(top=0.92, bottom=0.25, right=0.96)

    plt.savefig("A2.svg", dpi=300)

    #plt.show()

def prepare_B1():
    if os.path.isfile('data_csv/B1-prirustky_kraj.csv'):
        df = pd.read_csv('data_csv/B1-prirustky_kraj.csv', delimiter=";")
        plot_B1(df)
        print_B1(df)
    else:

        nakazeni_zacatek = None
        nuts = None
        zacatek = None
        for doc in cursor:
            if nuts == doc['kraj_nuts_kod']:
                if zacatek is not None:
                    nakazeni_prirustek = doc['kumulativni_pocet_nakazenych'] - nakazeni_zacatek
                    writer.writerow([
                        zacatek,
                        doc['datum'],
                        doc['kraj_nuts_kod'],
                        nakazeni_prirustek
                    ])
                    zacatek = None
                    continue
            else:
                nuts = doc['kraj_nuts_kod']

            zacatek = doc['datum']
            nakazeni_zacatek = doc['kumulativni_pocet_nakazenych']


def plot_B1(df):
    df['datum_zacatek'] = pd.to_datetime(df['datum_zacatek'])
    mask = df['datum_zacatek'] == '2020-10-01'
    df = df[mask].copy()
    df['pomer'] = df['nakazeni_prirustek'] / df['kraj_populace']
    df.sort_values(by=['pomer'], inplace=True, ascending=False)

    fig = plt.figure(figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)

    ind = np.arange(df.shape[0])
    width = 0.4

    ax.plot(df['kraj_nazev'].to_numpy(), df['pomer'].to_numpy(), color='g', label="Pomer nakazeny/pocet obyvatelov")
    ax2 = ax.twinx()
    ax2.bar(ind - width/2, df['kraj_populace'].to_numpy(), width=width, color='b', align='center', label="pocet obyvatelov")
    ax2.bar(ind + width/2, df['nakazeni_prirustek'].to_numpy(), width=width, color='r', align='center', label="nakazeny")

    ax2.set_ylim(0, 1700000)
    ax.set_ylim(0.045, 0.085)

    for label in ax.get_xticklabels(which='both'):
        label.set(rotation=30, horizontalalignment='right')
    ax.set_xticklabels(df['kraj_nazev'].to_numpy())

    ax.set_ylabel('Nakazeny na pocet obyvatelov')
    ax2.set_ylabel('Počet')
    fig.suptitle('Dotaz B1', fontsize=20)
    ax.legend(loc = 'upper left')
    ax2.legend()

    plt.subplots_adjust(top=0.92, bottom=0.25, right=0.85)

    plt.savefig("B1.svg", dpi=300)

    #plt.show()

def print_B1(df):

    df['datum_zacatek'] = pd.to_datetime(df['datum_zacatek'])
    df['pomer'] = df['nakazeni_prirustek'] / df['kraj_populace']
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('display.precision', 5)

    dates = df["datum_zacatek"].unique()

    with open('B1.txt', 'w') as f:
        for x in dates:
            mask = df['datum_zacatek'] == x
            ndf = df[mask].copy()
            ndf.index = np.arange(0, len(ndf))
            ndf.sort_values(by=['pomer'], inplace=True, ascending=False)
            print(pd.to_datetime(str(x)).strftime("%b %Y"), "-", pd.to_datetime(str(ndf['datum_konec'][0])).strftime("%b %Y"), file=f)
            ndf = ndf[['kraj_nazev', 'kraj_populace', 'nakazeni_prirustek','pomer']]
            ndf.index = np.arange(1, len(ndf)+1)
            print(ndf, file=f)
            print(file=f)


def prepare_C1():
    df = pd.read_csv('data_csv/C1-orp_ctvrtleti.csv', delimiter=";")
    df['datum_zacatek'] = pd.to_datetime(df['datum_zacatek'])
    df['datum_konec'] = pd.to_datetime(df['datum_konec'])

    dates = df["datum_zacatek"].unique()

    fdf = pd.DataFrame()

    for d in dates:
        mask = df['datum_zacatek'] == d
        ndf = df[mask].copy()
        ndf.index = np.arange(0, len(ndf))

        y = ndf[['0-14', '15-59', '60+']].copy()

        down_quantiles = y.quantile(0.05)
        outliers_low = (y < down_quantiles)
        y.mask(outliers_low, down_quantiles, axis=1, inplace=True)

        up_quantiles = y.quantile(0.95)
        outliers_high = (y >
        up_quantiles)
        y.mask(outliers_high, up_quantiles, axis=1, inplace=True)

        ndf.update(y)


        x = ndf['nakazeni'].values #returns a numpy array
        min_max_scaler = preprocessing.MinMaxScaler()
        x = x.reshape(-1,1)
        x_scaled = min_max_scaler.fit_transform(x)
        ndf['nakazeni'].update(x_scaled.reshape(-1))

        ndf['pocet_davek_discrete'] = pd.qcut(ndf['pocet_davek'], q=3, duplicates='drop', labels=["bad", "medium", "good"])

        ndf[['0-14', '15-59', '60+']] = ndf[['0-14', '15-59', '60+']].apply(np.int64)


        fdf = pd.concat([fdf,ndf])

    fdf.index = np.arange(0, len(fdf))
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    fdf.to_csv('C1.csv', index=False, sep=';')



def plot_D1():
    df = pd.read_csv('data_csv/custom1-zemreli_cr.csv', delimiter=";")
    df['datum_zacatek'] = pd.to_datetime(df['datum_zacatek'])
    df['pomer'] = df['zemreli_covid'] / df['zemreli']*100
    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())

    ax.plot(df['datum_zacatek'].to_numpy(), df['pomer'].to_numpy())
    plt.show()

def plot_D2():
    df = pd.read_csv('data_csv/custom2-zemreli-vekove-kategorie.csv', delimiter=";")

    df['pomer'] = df['umrti_covid'] / df['pocet_obyvatel']*100

    fig = plt.figure()
    ax = fig.add_subplot(1,1,1)
    ax.bar(df['vekova_kategorie'].to_numpy(), df['pomer'].to_numpy())
    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    plt.show()

plot_A1()
plot_A2()
prepare_B1()
prepare_C1()
#plot_D1()
#plot_D2()

