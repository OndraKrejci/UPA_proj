##
# @file prepare_dm.py
# @author Oliver Kuník xkunik00@stud.fit.vutbr.cz
# Subject: UPA - Data Storage and Preparation
# @date: 12/2021
# Prepare data from a CSV file for data mining

import pandas as pd
import numpy as np
from sklearn import preprocessing

from plot_graphs import get_ouput_path

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

if __name__ == '__main__':
    prepare_C1()
