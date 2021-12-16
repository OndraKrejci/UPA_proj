#!/usr/bin/env python3
##
# @file plot_graphs.py
# @author Oliver Kuník xkunik00@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 12/2021
# Plot graphs from CSV files

import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.ticker as mtick
import matplotlib.dates as mdates
import matplotlib.pyplot as plt

import os
import locale

locale.setlocale(locale.LC_ALL, 'cs_CZ.utf8')

def get_ouput_path(fname):
    OUTPUT_FOLDER = 'output/'
    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    return os.path.join(OUTPUT_FOLDER, fname)

def df_from_csv(fname):
    INPUT_FOLDER = 'data_csv/'
    path = os.path.join(INPUT_FOLDER, fname)
    return pd.read_csv(path, delimiter=';', encoding='utf-8')

def plot_A1():
    df = df_from_csv('A1-covid_po_mesicich.csv')

    df['zacatek'] = pd.to_datetime(df['zacatek'])

    figname = 'Dotaz A1'
    fig = plt.figure(figname, figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)

    ax.plot(df['zacatek'].to_numpy(), df['nakazeni'].to_numpy(), label='Nakažení')
    ax.plot(df['zacatek'].to_numpy(), df['vyleceni'].to_numpy(), label='Vyléčení')
    ax.plot(df['zacatek'].to_numpy(), df['hospitalizovani'].to_numpy(), label='Hospitalizovaní')
    ax.plot(df['zacatek'].to_numpy(), df['testy'].to_numpy(), label='Testy')

    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(mtick.StrMethodFormatter('{x:.0f}'))

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
    fig.suptitle(figname, fontsize=20)
    ax.legend()
    plt.subplots_adjust( left=0.15, right=0.98)

    plt.savefig(get_ouput_path('A1.svg'), dpi=300)

    #plt.show()

def plot_A2():
    df = df_from_csv('A2-osoby_nakazeni_kraj.csv')

    #replace NaN
    df['kraj_nazev'].replace(np.nan, 'neznámý', regex=True, inplace=True)

    #drop outliers
    Q1 = df['vek'].quantile(0.25)
    Q3 = df['vek'].quantile(0.75)
    IQR = Q3 - Q1

    df = df[~((df['vek'] < (Q1 - 1.5 * IQR)) |(df['vek'] > (Q3 + 1.5 * IQR)))]

    fig, ax = plt.subplots(figsize=(10, 6), dpi=150)
    figname = 'Dotaz A2'
    fig.canvas.set_window_title(figname)

    ax = sns.boxplot(ax=ax, x="kraj_nazev", y="vek", data=df)
    ax.set(xlabel=None, ylabel='Věk')
    for label in ax.get_xticklabels(which='both'):
        label.set(rotation=30, horizontalalignment='right')

    ax.set_title(figname, fontsize=20)
    plt.subplots_adjust(top=0.92, bottom=0.25, right=0.96)

    plt.savefig(get_ouput_path('A2.svg'), dpi=300)

    #plt.show()

def prepare_B1():
    df = df_from_csv('B1-nakazeni_kumulativne_kraj.csv')

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

    return df

def plot_B1(df):
    dates = df["datum_zacatek"].unique()
    mask = df['datum_zacatek'] == dates[1]
    df = df[mask].copy()
    df['pomer'] = df['nakazeni_prirustek'] / df['kraj_populace']
    df.sort_values(by=['pomer'], inplace=True, ascending=False)

    figname = 'Dotaz B1'
    fig = plt.figure(figname, figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)

    ind = np.arange(df.shape[0])
    width = 0.4

    ax.plot(df['kraj_nazev'].to_numpy(), df['pomer'].to_numpy(), color='g', label="Poměr nakažení/počet obyvatel")
    ax2 = ax.twinx()
    ax2.bar(ind - width/2, df['kraj_populace'].to_numpy(), width=width, color='b', align='center', label="počet obyvatel")
    ax2.bar(ind + width/2, df['nakazeni_prirustek'].to_numpy(), width=width, color='r', align='center', label="nakažení")

    ax2.yaxis.set_major_formatter(mtick.StrMethodFormatter('{x:.0f}'))

    ax2.set_ylim(0, 1600000)
    ax.set_ylim(0.051, 0.119)

    for label in ax.get_xticklabels(which='both'):
        label.set(rotation=30, horizontalalignment='right')
    ax.xaxis.set_major_locator(mtick.FixedLocator(range(0, df['kraj_nazev'].unique().size)))
    ax.set_xticklabels(df['kraj_nazev'].to_numpy())

    ax.set_ylabel('Poměr nakažení/počet obyvatel')
    ax2.set_ylabel('Počet')
    fig.suptitle(figname, fontsize=20)
    ax.legend(loc = 'upper left')
    ax2.legend()

    plt.subplots_adjust(top=0.92, bottom=0.25, right=0.85)

    plt.savefig(get_ouput_path('B1.svg'), dpi=300)

    #plt.show()

def print_B1(df, export=False):
    df['pomer'] = df['nakazeni_prirustek'] / df['kraj_populace']
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')
    pd.set_option('display.precision', 5)

    dates = df["datum_zacatek"].unique()

    if export:
        f = open(get_ouput_path('B1-tabulky.txt'), 'w', encoding='utf-8')

    for x in dates:
        mask = df['datum_zacatek'] == x
        ndf = df[mask].copy()
        ndf.index = np.arange(0, len(ndf))
        ndf.sort_values(by=['pomer'], inplace=True, ascending=False)

        print(pd.to_datetime(str(x)).strftime("%b %Y"), "-", pd.to_datetime(str(ndf['datum_konec'][0])).strftime("%b %Y"))
        if export:
            print(pd.to_datetime(str(x)).strftime("%b %Y"), "-", pd.to_datetime(str(ndf['datum_konec'][0])).strftime("%b %Y"), file=f)

        ndf = ndf[['kraj_nazev', 'kraj_populace', 'nakazeni_prirustek','pomer']]
        ndf.index = np.arange(1, len(ndf)+1)
        ndf.rename({
                'kraj_nazev': 'Název kraje',
                'kraj_populace': 'Počet obyvatel',
                'nakazeni_prirustek': 'Přírůstek nakažených',
                'pomer': 'Poměr'
            },
            axis=1, inplace=True
        )

        print(ndf)
        print()
        if export:
            print(ndf, file=f)
            print(file=f)

    if export:
        f.close()

def plot_D1():
    df = df_from_csv('D1-zemreli_cr.csv')

    df['datum_zacatek'] = pd.to_datetime(df['datum_zacatek'])
    df['pomer'] = df['zemreli_covid'] / df['zemreli']*100

    figname = 'Dotaz D1'
    fig = plt.figure(figname, figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)

    ax.plot(df['datum_zacatek'].to_numpy(), df['pomer'].to_numpy(), label="Poměr covid úmrtí/celková úmrtí")

    ax.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b \'%y'))
    ax.xaxis.set_minor_formatter(mdates.DateFormatter('%b'))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_minor_locator(mdates.MonthLocator(interval=1))
    fig.suptitle(figname, fontsize=20)
    ax.legend()

    for label in ax.get_xticklabels(which='both'):
        label.set(rotation=30, horizontalalignment='right')

    plt.savefig(get_ouput_path('D1.svg'), dpi=300)

def plot_D2():
    df = df_from_csv('D2-zemreli_vekove_kategorie.csv')

    df['pomer'] = df['umrti_covid'] / df['pocet_obyvatel']*100

    figname = 'Dotaz D2'
    fig = plt.figure(figname, figsize=(10, 6), dpi=150)
    ax = fig.add_subplot(1,1,1)
    ax.bar(df['vekova_kategorie'].to_numpy(), df['pomer'].to_numpy(), label="Poměr covid úmrtí/populace")

    ax.set_yscale('log')
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(decimals=3))
    ax.set_ylim(0.0008, 10.9)

    fig.suptitle(figname, fontsize=20)
    ax.set(xlabel='Věk', ylabel=None)
    ax.legend()
    plt.savefig(get_ouput_path('D2.svg'), dpi=300)

if __name__ == '__main__':
    df_B1 = prepare_B1()

    plot_A1()
    plot_A2()
    plot_B1(df_B1)
    print_B1(df_B1, export=True)
    plot_D1()
    plot_D2()

    plt.show()
