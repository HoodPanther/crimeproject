import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import date


def split_years(DF):
    DF = DF.copy()
    years_list= []
    years_list.append(DF.loc[DF['Day']<date(2007,1,1)])
    years_list.append(DF.loc[(DF['Day']>=date(2007,1,1)) & (DF['Day']<date(2008,1,1))])
    years_list.append(DF.loc[(DF['Day']>=date(2008,1,1)) & (DF['Day']<date(2009,1,1))])
    years_list.append(DF.loc[(DF['Day']>=date(2009,1,1)) & (DF['Day']<date(2010,1,1))])
    years_list.append(DF.loc[(DF['Day']>=date(2010,1,1)) & (DF['Day']<date(2011,1,1))])
    years_list.append(DF.loc[(DF['Day']>=date(2011,1,1)) & (DF['Day']<date(2012,1,1))])
    years_list.append(DF.loc[(DF['Day']>=date(2012,1,1)) & (DF['Day']<date(2013,1,1))])
    years_list.append(DF.loc[(DF['Day']>=date(2013,1,1)) & (DF['Day']<date(2014,1,1))])
    years_list.append(DF.loc[(DF['Day']>=date(2014,1,1)) & (DF['Day']<date(2015,1,1))])
    years_list.append(DF.loc[DF['Day']>=date(2015,1,1)])
    return years_list

def plots(years, title):
    sns.set(style="white")

    fig, (ax06, ax07, ax08, ax09, ax10, ax11, ax12, ax13, ax14, ax15) = \
          plt.subplots(10, sharey=True, figsize=(18,40))

    ax06.plot(years[0]["Day"], years[0]["Count"])
    ax06.set_title('2006')

    ax07.plot(years[1]["Day"], years[1]["Count"])
    ax07.set_title('2007')

    ax08.plot(years[2]["Day"], years[2]["Count"])
    ax08.set_title('2008')

    ax09.plot(years[3]["Day"], years[3]["Count"])
    ax09.set_title('2009')

    ax10.plot(years[4]["Day"], years[4]["Count"])
    ax10.set_title('2010')

    ax11.plot(years[5]["Day"], years[5]["Count"])
    ax11.set_title('2011')

    ax12.plot(years[6]["Day"], years[6]["Count"])
    ax12.set_title('2012')

    ax13.plot(years[7]["Day"], years[7]["Count"])
    ax13.set_title('2013')

    ax14.plot(years[8]["Day"], years[8]["Count"])
    ax14.set_title('2014')

    ax15.plot(years[9]["Day"], years[9]["Count"])
    ax15.set_title('2015')

    fig.suptitle(title, fontsize='x-large')
    fig.subplots_adjust(top=0.95)
    
    sns.despine(bottom=True, left=True)