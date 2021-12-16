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
import matplotlib.ticker as mtick
from sklearn import preprocessing
from matplotlib.ticker import StrMethodFormatter
import matplotlib.dates as mdates
import os

import locale
locale.setlocale(locale.LC_ALL, 'cs_CZ.utf8')

def get_ouput_path(fname):
    OUTPUT_FOLDER = 'output/'
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    return os.path.join(OUTPUT_FOLDER, fname)

def plot_A1():
    df = pd.read_csv('data_csv/A1-covid_po_mesicich.csv', delimiter=";")

    df['zacatek'] = pd.to_datetime(df['zacatek'])

    fig = plt.figure(figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)

    ax.plot(df['zacatek'].to_numpy(), df['nakazeni'].to_numpy(), label='Nakažení')
    ax.plot(df['zacatek'].to_numpy(), df['vyleceni'].to_numpy(), label='Vyléčení')
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

    plt.savefig(get_ouput_path('A1.svg'), dpi=300)

    #plt.show()

def plot_A2():
    df = pd.read_csv('data_csv/A2-osoby_nakazeni_kraj.csv', delimiter=";")

    #replace NaN
    df['kraj_nazev'].replace(np.nan, 'neznámý', regex=True, inplace=True)

    #drop outliers
    Q1 = df['vek'].quantile(0.25)
    Q3 = df['vek'].quantile(0.75)
    IQR = Q3 - Q1

    df = df[~((df['vek'] < (Q1 - 1.5 * IQR)) |(df['vek'] > (Q3 + 1.5 * IQR)))]

    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)

    ax = sns.boxplot(ax=ax, x="kraj_nazev", y="vek", data=df)
    ax.set(xlabel=None, ylabel='Věk')
    for label in ax.get_xticklabels(which='both'):
        label.set(rotation=30, horizontalalignment='right')

    ax.set_title('Dotaz A2')
    plt.subplots_adjust(top=0.92, bottom=0.25, right=0.96)

    plt.savefig(get_ouput_path('A2.svg'), dpi=300)

    #plt.show()

def prepare_B1():
    df = pd.read_csv('data_csv/B1-nakazeni_kumulativne_kraj.csv', delimiter=";")

    df['datum'] = pd.to_datetime(df['datum'])
    df['shift'] = df['kumulativni_pocet_nakazenych'].shift(periods=-1, fill_value=0)
    df['nakazeni_prirustek'] = df['shift']-df['kumulativni_pocet_nakazenych']
    df['datum_konec'] = df['datum'].shift(periods=-1)
    dates = df["datum"].unique()
    mask = ~(df['datum'] == dates[-1])
    df = df[mask].copy()
    df.drop(['shift', 'kumulativni_pocet_nakazenych'], axis=1, inplace=True)

    cols = df.columns.tolist()
    cols = cols[0:1] + cols[-1:] + cols[1:-1]
    df = df[cols]

    df['datum_konec'] = df['datum_konec'] - pd.Timedelta(days=1)
    df.rename({'datum': 'datum_zacatek'}, axis=1, inplace=True)

    plot_B1(df)
    print_B1(df)




def plot_B1(df):
    dates = df["datum_zacatek"].unique()
    mask = df['datum_zacatek'] == dates[1]
    df = df[mask].copy()
    df['pomer'] = df['nakazeni_prirustek'] / df['kraj_populace']
    df.sort_values(by=['pomer'], inplace=True, ascending=False)

    fig = plt.figure(figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)

    ind = np.arange(df.shape[0])
    width = 0.4

    ax.plot(df['kraj_nazev'].to_numpy(), df['pomer'].to_numpy(), color='g', label="Poměr nakažení/počet obyvatel")
    ax2 = ax.twinx()
    ax2.bar(ind - width/2, df['kraj_populace'].to_numpy(), width=width, color='b', align='center', label="počet obyvatel")
    ax2.bar(ind + width/2, df['nakazeni_prirustek'].to_numpy(), width=width, color='r', align='center', label="nakažení")

    ax2.yaxis.set_major_formatter(StrMethodFormatter('{x:.0f}'))

    ax2.set_ylim(0, 1600000)
    ax.set_ylim(0.051, 0.119)

    for label in ax.get_xticklabels(which='both'):
        label.set(rotation=30, horizontalalignment='right')
    ax.set_xticklabels(df['kraj_nazev'].to_numpy())

    ax.set_ylabel('Poměr nakažení/počet obyvatel')
    ax2.set_ylabel('Počet')
    fig.suptitle('Dotaz B1', fontsize=20)
    ax.legend(loc = 'upper left')
    ax2.legend()

    plt.subplots_adjust(top=0.92, bottom=0.25, right=0.85)

    plt.savefig(get_ouput_path('B1.svg'), dpi=300)

    #plt.show()

def print_B1(df):

    df['pomer'] = df['nakazeni_prirustek'] / df['kraj_populace']
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('display.precision', 5)

    dates = df["datum_zacatek"].unique()

    with open(get_ouput_path('B1.txt'), 'w', encoding='utf-8') as f:
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

        Q1 = y.quantile(0.25)
        Q3 = y.quantile(0.75)
        IQR = Q3 - Q1
        outliers_low = (y < Q1 - 1.5 * IQR)
        y.mask(outliers_low, Q1 - 1.5 * IQR, axis=1, inplace=True)

        outliers_high = (y > Q3 + 1.5 * IQR)
        y.mask(outliers_high, Q3 + 1.5 * IQR, axis=1, inplace=True)

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
    fdf.to_csv(get_ouput_path('C1.csv'), index=False, sep=';')



def plot_D1():
    df = pd.read_csv('data_csv/D1-zemreli_cr.csv', delimiter=";")
    df['datum_zacatek'] = pd.to_datetime(df['datum_zacatek'])
    df['pomer'] = df['zemreli_covid'] / df['zemreli']*100

    fig = plt.figure(figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)

    ax.plot(df['datum_zacatek'].to_numpy(), df['pomer'].to_numpy(), label="Poměr covid úmrtí/úmrtí")

    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b \'%y'))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter('%b'))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))
    fig.suptitle('Dotaz D1', fontsize=20)
    ax.legend()

    for label in ax.get_xticklabels(which='both'):
        label.set(rotation=30, horizontalalignment='right')

    plt.savefig(get_ouput_path('D1.svg'), dpi=300)

def plot_D2():
    df = pd.read_csv('data_csv/D2-zemreli_vekove_kategorie.csv', delimiter=";")

    df['pomer'] = df['umrti_covid'] / df['pocet_obyvatel']*100

    fig = plt.figure(figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)
    ax.bar(df['vekova_kategorie'].to_numpy(), df['pomer'].to_numpy(), label="Poměr covid úmrtí/populace")

    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=3))
    ax.set_ylim(0.0008, 10.9)

    fig.suptitle('Dotaz D2', fontsize=20)
    ax.set(xlabel='Věk', ylabel=None)
    ax.legend()
    plt.savefig(get_ouput_path('D2.svg'), dpi=300)

if __name__ == '__main__':
    plot_A1()
    plot_A2()
    prepare_B1()
    prepare_C1()
    plot_D1()
    plot_D2()
    plt.show()
