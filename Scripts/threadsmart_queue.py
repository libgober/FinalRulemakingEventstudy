#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 17:41:24 2017

Uses a locking mechanism
http://fasteners.readthedocs.io/en/latest/examples.html

@author: brianlibgober
"""

import os
import pandas as pd
import fasteners
import apsw
import subprocess as sp

#folder where the extracted databases should go
#%%
#for each stock this is the parameter combination we will want
times = pd.read_csv("timestamps.csv",names=["stamp"])
todo = pd.read_csv("todo_list.csv",names=["stock"])

MAINQUERY = \
"""
/* SQL Code Written on October 19, 2017. 

This code was developed on dBeaver, using

~/Dropbox/Collaborations/C_outcome_neutral.sql
~/Dropbox/Collaborations/C.sqlite

both synced to Dropbox on the day above prior to 8:02 AM. This block of text contains 
minor edits.  The only significant difference here is that :outcome is 
replaced with {outcome} and formatted by python,
because the apsw package couldn't handle it (although dbeaver could).
Also changed the ordering of outputs.

This is a long query with that uses many temporary views. 
In order to illustrate what each view does, the returned values have been shown
assuming the database above and the following parameters for the query.  
Simply remove the /* comment line and the expected query is returned.

The following parameters were used:
* :trademinstart is 1281445200, or for humans 2010-08-10 13:00:00
* :duration is 3600
* :outcome is PATH_SP500_EW
* :count is 201
* :sym_root is "C", quotes important or else dBeaver thinks is a column
* :market_model is "RSP" (recommend either RSP=SP500_EW, VTI=Total Market, None)


*/

with 
initialquotes as (
--this table contains the times to use for t0 for each of up to :count days prior
--for each day, it finds the last quote before the event time
select max(datetime(time,'unixepoch')) as t0 from mp
where 
time<=:trademinstart -- no need to look after the event time
and 
time(time,'unixepoch')<=time(:trademinstart,'unixepoch')
and 
{outcome} is NOT NULL
group by date(time,'unixepoch')
order by t0 desc
limit :count
)
/*select * from initialquotes limit 3;
***
2010-08-10 13:00:00
2010-08-09 13:00:00
2010-08-06 13:00:00
***
*/
,finalquotes as (
---this table contains the times to use for t1 for each of up to :count days prior
---for each day, it finds the last quote before the event time + duration
select max(datetime(time,'unixepoch')) as t1 from mp
where 
time<=:trademinstart+:duration and
time(time,'unixepoch')<=time(:trademinstart+:duration,'unixepoch')
and {outcome} is not NULL
group by date(time,'unixepoch')
order by t1 desc
limit :count
)
/*select * from finalquotes limit 3;
***
2010-08-10 13:05:00
2010-08-09 13:05:00
2010-08-06 13:05:00
***
*/
,pricelist as ( 
--now that we know the times we want prices for, we simply join these times
--to the full series
select
	t0,t1,
	A.{outcome} as p0,
	B.{outcome} as p1,
	B.{outcome}-A.{outcome} as d
from initialquotes
left join finalquotes
on date(t0)=date(t1)
left join mp A
on initialquotes.t0=datetime(A."time",'unixepoch')
left join mp B
on finalquotes.t1=datetime(B."time",'unixepoch')
)
/*select * from pricelist limit 3;
t0                  |t1                  |p0                  |p1                  |d                      |
--------------------|--------------------|--------------------|--------------------|-----------------------|
2010-08-10 13:00:00 |2010-08-10 14:00:00 |-3.2693789519492107 |-3.2588199511125127 |0.01055900083669803    |
2010-08-09 13:00:00 |2010-08-09 14:00:00 |-3.3363619819013293 |-3.312884649782538  |0.023477332118791328   |
2010-08-06 13:00:00 |2010-08-06 14:00:00 |-3.1442862480379006 |-3.1468922225827107 |-0.0026059745448101523 |
 */
,comparisons as (select d,d0 from pricelist
cross join (select d as d0 from pricelist where date(:trademinstart,'unixepoch')=date(t0)) B 
limit -1 offset 1
)
/*select * from comparisons limit 3;
d                      |d0                  |
-----------------------|--------------------|
0.023477332118791328   |0.01055900083669803 |
-0.0026059745448101523 |0.01055900083669803 |
-0.013655092018214798  |0.01055900083669803 |
 */
--calculations
select 
:sym_root as sym_root,
date(:trademinstart,'unixepoch') as eventdate,
time(:trademinstart,'unixepoch') as eventtime,
:trademinstart as unixtime,
:market_model as market_model,
:duration as duration,
(avg(d<=d0)+avg(d<d0))/(2)as q,
count(*) as n,
avg(d) as m,
stdev(d) as s,
max(d0) as r,
(max(d0)-avg(d))/stdev(d) as t from comparisons;
/*
sym_root |eventdate  |eventtime |market_model |unixtime   |duration |q                  |n   |m                     |s                   |r                   |t                  |
---------|-----------|----------|-------------|-----------|---------|-------------------|----|----------------------|--------------------|--------------------|-------------------|
C        |2010-08-10 |13:00:00  |RSP          |1281445200 |3600     |0.7284768211920529 |151 |-0.006077189104862718 |0.03367658595720302 |0.01055900083669803 |0.4939987076689544 |

Runtime of 392ms, several orders of magnitude faster than the R code we used at first. 

We wish to run for each of 60 minutes, for each of 600 stocks, for each of 3 market models,
for each date (152 by last count).

WRDS gives access to 24*5 cores, each of which should be able to do this in parallel.
Back of the envelope calculation assuming no inefficiency in parallelizing/1 sec per query

(60*600*3*150)/(120*60*60)=38 hours as suggested runtime. 
*/
"""

def refresh_claims():
    """
    Get a new list of what's been claimed
    """
    try:
        claimed = pd.read_csv("claimed.csv")
    except:
        claimed = pd.DataFrame([],columns=['stock'])
    return claimed

def check_if_any_tasks(todo,claimed):
    """
    Checks whether there is anything on the to do list that hasn't been claimed
    """
    return any(~todo.stock.isin(claimed.stock))

#%%
claimed = refresh_claims()
a_lock = fasteners.InterProcessLock(os.path.expanduser('~/tmp_lock_file'))
#proceed on assumption that there is something to do
while check_if_any_tasks(todo,claimed):
    ##############  MANAGING MULTIPLE THREADS #####################
    #should cause program to wait until it can acquire the lock
    gotten = a_lock.acquire()
    print "Lock obtained, we will have the most current claims file"
    claimed = refresh_claims()
    #if having obtained the lock and read the most current version
    #see there is nothing to do, we can release the lock and end
    if not check_if_any_tasks(todo,claimed):
        a_lock.release()
        break
    #otherwise claim the first stock
    todo_current = todo.stock[~todo.stock.isin(claimed.stock)]
    stock = todo_current[min(todo_current.index)]
    claimed = claimed.append({"stock" :stock},ignore_index=True)
    claimed.to_csv("claimed.csv",index=False)
    a_lock.release()
    print "Lock released. Succesfully claimed", stock 
    ##############  LOADING IN MEMORY SQLITE DB    ################# 
    lzma_db_filename =  "./Data/{stock}.sqlite.lzma".format(stock=stock)
    db_filename = "./Data/{stock}.sqlite".format(stock=stock)
    analysis_filename = "./Analysis/{stock}.csv".format(stock=stock)
    #unzip the sqlite file
    sp.call(["lzma","-dk"],stdin=open(lzma_db_filename),stdout=open(db_filename,"w+"))
    print "Uncomprresed sqlite file"
    #we will load the sqlite file into memory for even greater performance 
    conn = apsw.Connection(":memory:")
    disk_conn = apsw.Connection(db_filename)
    # Copy the disk database into memory
    print "Loading db into memory"
    with conn.backup("main", disk_conn, "main") as backup:
        backup.step() # copy whole database in one go
    print"Done! Cleaning up..."
    #the database is in memory, so the disk files we created can go now
    del disk_conn
    sp.call(["rm",db_filename])    
    ##############  OPERATIONS ON  DB             ################# 
    conn.enableloadextension(True)
    conn.loadextension(os.path.expanduser("~/libsqlitefunctions"))
    #build a list of parameters to supply to the query
    PARAMS = []
    minutes = range(1,61) + [120,24*60]
    for i in times.stamp:
        for m in [i for i xrange(1,61):
            PARAMS.append(
                    {"trademinstart":i,
                     "duration" : 60*m,
                     "sym_root" : stock,
                     "count" : 200 #No. of comparison days
                     })    
    #get a cursor
    c =conn.cursor()
    print "Starting analysis"
    alldata = []
    for outcome,market_model in [("PATH_SP500_EW","RSP"),
                                 ("PATH_TOTALMARKET","VTI"),
                                 ("quote","None")]:
        for param in PARAMS:
            param["market_model"] = market_model
        QUERY = MAINQUERY.format(outcome=outcome)
        print "Starting",market_model
        data = c.executemany(QUERY, PARAMS).fetchall()
        data = pd.DataFrame(data,columns=["sym_root","eventdate","eventtime",
                                          "unixtime","market_model",
                                   "duration","q","n","m","s","r","t"])
        alldata.append(data)
    pd.concat(alldata).to_csv(analysis_filename,index=False)
    print "Done! Grabbing next stock"
    
print "Nothing left to do! Exiting..."
