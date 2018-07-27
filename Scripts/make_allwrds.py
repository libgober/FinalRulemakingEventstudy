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
conn = create_engine("sqlite:///Data/frd.sqlite")

#%% HELPERS

def block_until_complete():
    result = int(sp.check_output('qstat | wc -l',shell=True))
    while result != 0:
        sleep(10)
        result = int(sp.check_output('qstat | wc -l',shell=True))
        
def make_series(symbol,req):
    #the script
    request=req.format(symbol=symbol,month="",day="",starttime="9:35",endtime="16:00")
    with open("{symbol}.sas".format(symbol=symbol),"w+") as f:
        f.write(request)
    sp.call("qsas {symbol}.sas".format(symbol=symbol),
            shell=True)

    
#%% REFERENCE CONSTANTS
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
	  taqmsec.nbbom_2018{month}{day}:
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


############### ASSEMBLE MINUTE SERIES FOR EACH STOCK #######################

bank_stocks = pd.read_csv("Data/Financial_Sector_Stocks_NASDAQ.csv",index_col=0)

#bankSymbols= bank_stocks.Symbol[(bank_stocks.Country == "United States") \
#                            & ~bank_stocks["Market Cap"].str.contains("n/a") \
#                            ].values

bankSymbols= bank_stocks.Symbol[~bank_stocks["Market Cap"].str.contains("n/a") \
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
Symbols = np.unique(np.concatenate([toadd,bankSymbols]))




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
                                        11:00 Thursday, October 12, 
					

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
        timing.earliest_time.dropna().values,
        timing.feedly_time.dropna().values,
        timing.wayback_time.dropna().values,
        timing.foia_time.dropna().values]))


    
    
pd.Series(discrete_times).to_csv("timestamps.csv",index=False)
"""
discrete_times[0] = '2010-08-10 13:00:00-0400'
timestamps[0] = 1281445200 #GMT: Tuesday, August 10, 2010 1:00:00 PM
"""

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
#%% setup our bootstrap placebo tests. 
days = pd.read_csv("Data/stock_days.csv")
#subset to days in the range of our than our dataset.
days = days[(days.caldt >= '2010-08-10') & (days.caldt <='2016-11-23')] 
#load participant stocks
participant_stocks = pd.read_excel("Data/participant_stocks.xlsx")
#ignore partsof this where there is no ticker
participant_stocks = participant_stocks[~participant_stocks.TICKERS.isnull()]
#capitalize every ticker for consistency
participant_stocks["TICKERS"]=participant_stocks.TICKERS.str.upper()
#setup to concatenate by rule into a big comma delimited list
grpby = participant_stocks.groupby(
        ['DocketNumber','DocketVersion'])
#join all tickers by group
bydocketv=grpby.agg(lambda x: ",".join(x))
#per docket, split and count unique bank stocks
bydocketv["participants"] = bydocketv.TICKERS.str.split(",").apply(
        lambda x: [i.strip() for i in x if i.strip() in bankSymbols]).apply(
        lambda x: np.unique(x).size)

participants_per_docket = bydocketv["participants"].reset_index()

#old code, useful if interested in placeboing comments rather than commenters
#count the bank stocks in each row
#participant_stocks["bank_commentsperrow"] = participant_stocks.TICKERS.str.split(
#        ",",expand=False).apply(
#                lambda x: len([i.strip() for i in x if i.strip() in bankSymbols]))
#get the number of participants by row.
#participants_by_row = participant_stocks.drop("CommentID",axis=1).groupby(
#        ['DocketNumber','DocketVersion']).sum()
#make multiindex into columns
#participants_by_row = participants_by_row.reset_index()


timing2 = pd.read_sql('timing',conn)

timing2["earliest_time"]=timing2.ix[:,
       ["foia_time","feedly_time","wayback_time"]].min(1)
timing2["latest_time"]=timing2.ix[:,
       ["foia_time","feedly_time","wayback_time"]].max(1)

#now make sure that we can observe
tmp = pd.to_datetime(timing2.earliest_time,unit='s')
tmp = tmp.dt.hour + tmp.dt.minute/60

tmp2 = pd.to_datetime(timing2.latest_time,unit='s')
tmp2 = tmp2.dt.hour + tmp2.dt.minute/60 


select=(tmp < 16) & (tmp2 > 9+35/60)
tmp3 = timing2[select & (timing2.Action.isin(['Proposed','Final']))]
template = pd.merge(tmp3[['Action','DocketNumber','DocketVersion']],
         participants_per_docket,
         how='left').fillna(0)



#%% now we need to setup our set of random times.
records=[]
for i in range(200):
    tmp = template.copy()
    np.random.seed(i)
    tmp['placebo_version'] = i
    #draw random trading dates
    for idx,row in tmp.iterrows():
        idx,row
        eventday = days.caldt.sample().values[0]
        eventhour = (16-(9+35./60))*np.random.rand() + 9+35./60
        row['unixtime']= arrow.get(eventday).\
                shift(hours=eventhour).\
                floor('minute').\
                timestamp
        stocks=np.random.choice(bankSymbols,size=int(row.participants),replace=False)
        row['allparticipatingstocks'] = ",".join(stocks)
        for stock in stocks:
            toadd = row.copy()
            toadd['TICKER'] = stock
            records.append(toadd.to_dict())
    
placebo_schedule = pd.DataFrame(records)
placebo_schedule.to_csv("Data/placebo_tests.csv")



#%% 
#first we must the series for the market funds
make_series("RSP",request)
make_series("VTI",request)
block_until_complete()
#now we can do the processing we need on the financial stocks
for symbol in Symbols:
    make_series(symbol,request+additional_SAS_commands)
block_until_complete()


sp.call("mkdir -f Analysis",shell=True)
sp.call("rm -f claimed.csv",shell=True)
sp.call("rm -f ~/tmp_lock_file",shell=True)
for i in xrange(1,6):
    call="qsub -cwd -N analyzer_no{i} -j y -b y " + \
    "'seq 24 | ~/anaconda2/bin/parallel -n0 ~/anaconda2/bin/python Scripts/threadsmart_queue.py'"
    sp.call(call.format(i=i),shell=True)

block_until_complete() 


sp.call("rm -f analysis.sqlite",shell=True)
conn2 = create_engine("sqlite:///analysis.sqlite")
for i in glob.glob("Analysis/*.csv"):
    d = pd.read_csv(i)
    d=d[~d.unixtime.isnull()]
    d.unixtime = d.unixtime.astype('int')
    d.to_sql("main",conn2,if_exists="append",index=False)
