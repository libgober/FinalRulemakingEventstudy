#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 17:51:18 2017

@author: brianlibgober

This file builds the data necessary to conduct the event study
as well as conducts it.

"""


################################## SETUP ######################################
import os
import pandas as pd
from datetime import datetime,timedelta
from sqlalchemy import create_engine
import numpy as np
import subprocess as sp
import arrow
import apsw
import glob
from time import sleep
os.chdir(os.path.expandvars("$REPFOLDER"))
conn = create_engine("sqlite:///frd.sqlite")

#%%

def block_until_complete():
    result = int(sp.check_output('qstat | wc -l',shell=True))
    while result != 0:
        sleep(10)
        result = int(sp.check_output('qstat | wc -l',shell=True))
    



############### ASSEMBLE MINUTE SERIES FOR EACH STOCK #######################

bank_stocks = pd.read_csv("Data/Financial_Sector_Stocks_NASDAQ.csv",index_col=0)

Symbols= bank_stocks.Symbol[(bank_stocks.Country == "United States") \
                            & ~bank_stocks["Market Cap"].str.contains("n/a") \
                            ].values
                            
participant_stocks =  pd.read_excel("Data/participant_stocks.xlsx")

toadd = []
for i in participant_stocks.TICKERS:
    if i!=i:
        continue
    else:
        if "," in i:
            toadd = toadd + i.upper().split(",")
        else:
            toadd = toadd + [i.upper()]
toadd=np.unique([i.strip() for i in toadd if i.strip() != ""])
Symbols = np.unique(np.concatenate([toadd,Symbols]))


#%%
request  = """
/* Acquire all minute by minute stock data from {symbol}
as a big CSV that can be imported into a SQL database*/
/* START DATA STEP */
data d1 (rename=(BB=BEST_BID BO=BEST_ASK SYMBOL=SYM_ROOT)) /view=d1;
      /* Define the dataset we will use as source */
      set taq.nbbo_2010{month}{day}: taq.nbbo_2011{month}{day}: taq.nbbo_2012{month}{day}: taq.nbbo_2013{month}{day}:
	(KEEP = Date TIME BB BO SYMBOL );
      /* no need to include irrelevant stuff */
      where
        BB <> 0 and BO <> 0 and SYMBOL='{symbol}'
          and TIME between "{starttime}"t and "{endtime}"t;
      /* add a grouping variable */
      TimeID = dhms(DATE,hour(TIME),minute(TIME),0);
      format TimeID Datetime15.;
      PRICE = ((BB + BO)/2);

data d2 (rename=(BEST_BID=BEST_BID BEST_ASK=BEST_ASK Sym_Root=SYM_ROOT)) /view=d2;
      /* Define the dataset we will use as source */
      set taqmsec.nbbom_2014{month}{day}: taqmsec.nbbom_2015{month}{day}: 
          taqmsec.nbbom_2016{month}{day}: taqmsec.nbbom_2017{month}{day}:
	(KEEP = Date Time_M BEST_BID BEST_ASK Sym_Root SYM_SUFFIX);
      /* no need to include irrelevant stuff */
      where
        BEST_BID <> 0 and BEST_ASK <> 0 and Sym_Root='{symbol}' and
        SYM_SUFFIX='' and Time_M between "{starttime}"t and "{endtime}"t;
      /* add a grouping variable */
      TimeID = dhms(DATE,hour(Time_M),minute(Time_M),0);
      format TimeID Datetime15.;
      PRICE = ((BEST_BID + BEST_ASK)/2);

data d /view=d;
set d1 d2;

 
      /*BEGIN SUMMARIZING      */
PROC MEANS NOPRINT DATA=d mean;
CLASS TimeID /Missing;
/* By default, Proc means looks at all 2^k combinations of k class variables.*/
/*We only want all three, and this command accomplishes that */
Types TimeID;
var PRICE;
/*OUTPUT DATASET NAME */
OUTPUT out=e;
run;


data f (DROP= _STAT_ 
        rename=(TimeID=time Price=quote));
  set e (KEEP = TimeID Price _STAT_);
  where _STAT_="MEAN";
  format TimeID;

PROC EXPORT data=f OUTFILE="Data/{symbol}.csv" DBMS=csv REPLACE;
run;
"""

additional_SAS_commands="""
X "~/anaconda2/bin/python ./Scripts/additional_processing.py {symbol}";
run;
"""







#%%
def make_series(symbol,req):
    #the script
    request=req.format(symbol=symbol,month="",day="",starttime="9:35",endtime="16:00")
    with open("{symbol}.sas".format(symbol=symbol),"w+") as f:
        f.write(request)
    sp.call("qsas {symbol}.sas".format(symbol=symbol),
            shell=True)

    
    
    
#%%
################################# DATA STEPS #################################
    
#first we must the series for the market funds
make_series("RSP",request)
make_series("VTI",request)
block_until_complete()
#now we can do the processing we need on the financial stocks
for symbol in Symbols:
    make_series(symbol,request+additional_SAS_commands)
block_until_complete()

#%%


# identify the times to study.
"""
An important issue to be mindful of is to do with time-zones. 
WRDS's NBBO data stores all times as if they were GMT. 

To see this, try the following command

---

data d1 (rename=(BB=BEST_BID BO=BEST_ASK SYMBOL=SYM_ROOT)) /view=d1;
set taq.nbbo_2010022: taq.nbbo_2011022: taq.nbbo_2012022: taq.nbbo_2013022:
(KEEP = Date TIME BB BO SYMBOL );
where
BB <> 0 and BO <> 0 and SYMBOL='C'
and TIME between "9:35"t and "10:00"t;
TimeID= dhms(DATE,hour(TIME),minute(TIME),0);

proc print data=d1 (obs=3); run;

---


                         The SAS System                                4
                                        11:00 Thursday, October 12, 2017

Obs  SYM_ROOT      DATE   TIME    BEST_BID  BBSize  BBASize  BBEXLIST

  1     C      20100222  9:35:00    3.42     11324   41432     ****
  2     C      20100222  9:35:01    3.42     11427   41731     ****
  3     C      20100222  9:35:02    3.42     11424   41764     ****

Obs  BEST_ASK    BOSize    BOASize    BOEXLIST    NUMEX      TimeID

  1    3.43       9753      35206       ****        10     1582450500
  2    3.43       9742      35394       ****        10     1582450500
  3    3.43       9742      35409       ****        10     1582450500
  
  
To get a unix time stamp, apply the correction to the TimeID, 
1582450500-315619200=1266831300

And any time stamp converter  (e.g. https://www.epochconverter.com/) reveals 


1266831300 => GMT: Monday, February 22, 2010 9:35:00 AM

But of course, that's not quite right...

Financial Rulemaking dataverse (FRD) records time with UTC offsets. 

The simplest way to make things is agree is to simply throw out FRD's timezone
information and pretend as if everything were happening in GMT time.  
This naive approach actually makes sense, since in truth rule announcement times
and stock times are both in American Eastern Time.  
"""


timing = pd.read_sql("""
                     select *,
                     min(foia_time,feedly_time,wayback_time) as earliest_time
                     from timing
                     """,conn)

#WRDS NBBO data records all times as if they were in UTC. 

#Therefore

discrete_times = np.unique(np.concatenate([
        timing.earliest_time.values,
        timing.feedly_time.values,
        timing.wayback_time,
        timing.foia_time]))

pd.Series(discrete_times).to_csv("timestamps.csv",index=False)
"""
discrete_times[0] = '2010-08-10 13:00:00-0400'
timestamps[0] = 1281445200 #GMT: Tuesday, August 10, 2010 1:00:00 PM
"""

#%% SETTING UP CONTENT ANALYSIS
#The times we are interested in are now ascertained.
#
#Now we will build another database that derives statistics for each time delta.
#We use pure SQL operations described in another file 
#
#It takes <1 second to calculate a single row of this table.
#However we must calculate 60 rows per event (120+) and there are 700 stocks.
#
#Should take only 58 days...
#
#Clearly this is pleasantly parallel. One can run 5 sessions on WRDS at a time.
#Each sessions gets 24 cores.  Therefore theoretical run time is like 11 hours. ;-)

#In order to parallelize, we will use GNU Parallel and a file system based queue
#This way it doesn't matter how many threads we actually have access to,
#we will always be using maximum resources.
#The basic idea is that timestamps.csv and todo_list.csv
#contain the two queues of what needs to be done.
#each thread-instance looks to see if another process has "claimed" a given stock symbol
#by writing in a 'claimed' queue
#if not claimed, then it claims it and proceeds to add it to its own
#parallel queue.


pd.Series(Symbols).to_csv("todo_list.csv",index=0)
#submit each multithreading enabled parser
#wait until completion
with open("make_stock_days.sas","w+") as f:
    f.write("""
data d (rename=(date=caldt));
set ff.factors_daily (keep=date);
format date yymmdd10.;

PROC EXPORT data=d OUTFILE="Data/stock_days.csv" DBMS=csv REPLACE;
run;
""")
    sp.call(["qsas","make_stock_days.sas"])
block_until_complete()
