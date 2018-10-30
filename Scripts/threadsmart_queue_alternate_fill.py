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
--1474662600
with initialprices as (
	select 
		time as t0,
		quote as p01,
		PATH_SP500_EW p02,
		PATH_TOTALMARKET p03 
	from mp 
	where 
	time(time,'unixepoch')=(select time(max("time"),'unixepoch') from mp where "time" <=:trademinstart) AND
	time<=(select max("time") from mp where "time" <=:trademinstart)
	order by t0 desc
	limit :count
	)
/*select * from initialprices limit 3;
t0         |p01                |p02                 |p03                 |
-----------|-------------------|--------------------|--------------------|
1474646400 |43.435408163000005 |-3.6885565573402195 |-3.3985007967091434 |
1474560000 |43.332073171000005 |-4.047691479711392  |-4.018898858668966  |
1474473600 |42.721363636       |-4.397766161265537  |-4.084025386992101  |
*/
,initialtimes as (
select a.t0,count(b.t0) idx from initialprices a join initialprices b on a.t0<=b.t0
group by a.t0)
/*select * from initialtimes order by idx asc;
t0         |idx |
-----------|----|
1474646400 |1   |
1474560000 |2   |
1474473600 |3   |*/
,finalprices as (
	select time as t1,
		quote as p11,
		PATH_SP500_EW p12,
		PATH_TOTALMARKET p13 from 
		mp where 
		time(time,'unixepoch')=(select time(min("time"),'unixepoch')  from mp where "time" >=:trademinstart+:duration) AND
		time<=(select min("time")  from mp where "time" >=:trademinstart+:duration)
		order by t1 desc
		limit :count
)
/*select * from finalprices limit 3;
t1         |p11          |p12                |p13                 |
-----------|-------------|-------------------|--------------------|
1474882500 |42.508372093 |-4.424911344129631 |-3.8919483824393346 |
1474623300 |43.338809524 |-3.987652300207605 |-3.869710429324103  |
1474536900 |43.305274725 |-4.167612655177663 |-4.111647085392955  |*/
,finaltimes as (
select a.t1,count(b.t1) idx from finalprices a join finalprices b on a.t1<=b.t1
group by a.t1)
/*select * from finaltimes order by idx asc;
t1         |idx |
-----------|----|
1474882500 |1   |
1474623300 |2   |
1474536900 |3   |
*/
,differences as (select 
	initialtimes.t0,
	initialtimes.idx as idx0,
	finaltimes.t1,
	finaltimes.idx as idx1,
	p11-p01 as d1,
	p12-p02 as d2,
	p13-p03 as d3
from 
	initialtimes join finaltimes on 
		initialtimes.idx=finaltimes.idx
	left join initialprices
		on initialtimes.t0=initialprices.t0
	left join finalprices
		on finaltimes.t1=finalprices.t1)
/*select datetime(t0,'unixepoch') t0,datetime(t1,'unixepoch') t1,idx0,idx1,d1,d2,d3 from differences order by idx0 asc limit 3;
t0                  |t1                  |idx0 |idx1 |d1                    |d2                  |d3                    |
--------------------|--------------------|-----|-----|----------------------|--------------------|----------------------|
2016-09-23 16:00:00 |2016-09-26 09:35:00 |1    |1    |-0.9270360700000069   |-0.7363547867894118 |-0.4934475857301912   |
2016-09-22 16:00:00 |2016-09-23 09:35:00 |2    |2    |0.0067363529999937555 |0.06003917950378668 |0.14918842934486332   |
2016-09-21 16:00:00 |2016-09-22 09:35:00 |3    |3    |0.5839110889999972    |0.23015350608787344 |-0.027621698400854378 |
*/
,comparisons as (select A.t0,A.idx0,A.t1,d1,d2,d3,B.idx0 idx00,B.t0 t00,B.t1 t11,B.e1,B.e2,B.e3 from differences A cross join 
(select idx0,t0,t1,d1 as e1,d2 e2,d3 e3 from differences where idx0=1) B 
order by A.idx0 asc
limit -1 offset 1)
/*select * from comparisons limit 3;
t0         |idx0 |t1         |d1                    |d2                  |d3                    |idx00 |t00        |t11        |e1                  |e2                  |e3                  |
-----------|-----|-----------|----------------------|--------------------|----------------------|------|-----------|-----------|--------------------|--------------------|--------------------|
1474560000 |2    |1474623300 |0.0067363529999937555 |0.06003917950378668 |0.14918842934486332   |1     |1474646400 |1474882500 |-0.9270360700000069 |-0.7363547867894118 |-0.4934475857301912 |
1474473600 |3    |1474536900 |0.5839110889999972    |0.23015350608787344 |-0.027621698400854378 |1     |1474646400 |1474882500 |-0.9270360700000069 |-0.7363547867894118 |-0.4934475857301912 |
1474387200 |4    |1474450500 |0.22968390799999838   |0.05393830891530982 |-0.09725395234286438  |1     |1474646400 |1474882500 |-0.9270360700000069 |-0.7363547867894118 |-0.4934475857301912 |
*/
,wideresults as (select 
     :sym_root as sym_root,
     date(:trademinstart,'unixepoch') as eventdate,
     time(:trademinstart,'unixepoch') as eventtime,
     :trademinstart as unixtime,
     :duration as duration,
     t00 as actual_event_start,
     datetime(t00,'unixepoch') as actual_event_start_human,
     t11 as actual_event_end,
     datetime(t11,'unixepoch') as actual_event_end_human,
	(avg(d1<=e1)+avg(d1<e1))/(2) as q1,
	sum(d1 is not NULL) as n1,
	avg(d1) as m1,
	stdev(d1) as s1,
	e1 as r1,  --e1 all identical so this is ok
	(e1-avg(d1))/stdev(d1) as t1,
	(avg(d2<=e2)+avg(d2<e2))/(2) as q2,
	sum(d2 is not NULL) as n2,
	avg(d2) as m2,
	stdev(d2) as s2,
	e2 as r2,  --e2 all identical so this is ok
	(e2-avg(d2))/stdev(d2) as t2,
	(avg(d3<=e3)+avg(d3<e3))/(2) as q3,
	sum(d3 is not NULL) as n3,
	avg(d3) as m3,
	stdev(d3) as s3,
	e3 as r3,  --e3 all identical so this is ok
	(e3-avg(d3))/stdev(d3) as t3 from comparisons)
select * from wideresults;
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

def process_rawdata(rawdata):
    """
    Turns out that going from wide to long with sqlite temporary views
    is pretty slow. Substantial time savings obtained via doing this transform
    in python.
    """
    common = pd.DataFrame(rawdata).iloc[:,0:9]
    common.columns = ["sym_root",
                      "eventdate",
                      "eventtime",
                      "unixtime",
                      'duration',
                      "actual_event_start",
                      "actual_event_start_human",
                      "actual_event_end",
                      "actual_event_end_human"]
    #first tmp is the simple returns
    simple=pd.DataFrame(rawdata).iloc[:,9:15]
    simple.columns = ['q','n','m','s','r','t']
    simple = pd.concat([common,simple],axis=1)
    simple["market_model"] = "None"
    #
    rsp=pd.DataFrame(rawdata).iloc[:,15:21]
    rsp.columns = ['q','n','m','s','r','t']
    rsp = pd.concat([common,rsp],axis=1)
    rsp["market_model"] = 'RSP'
    #vti
    vti = pd.DataFrame(rawdata).iloc[:,21:27]
    vti.columns =  ['q','n','m','s','r','t']
    vti = pd.concat([common,vti],axis=1)
    vti["market_model"] = 'VTI'
    out = pd.concat([simple,rsp,vti])
    return out

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
    #update the claimed file
    claimed = claimed.append({"stock" :stock},ignore_index=True)
    claimed.to_csv("claimed.csv",index=False)
    a_lock.release()
    print "Lock released. Succesfully claimed", stock 
    ##############  LOADING IN MEMORY SQLITE DB    ################# 
    lzma_db_filename =  "./Data/{stock}.sqlite.lzma".format(stock=stock)
    db_filename = "./Data/{stock}.sqlite".format(stock=stock)
    analysis_filename = "./Analysis/{stock}.csv".format(stock=stock)
    #sometimes a sqlite file is not generated, these must be
    #separately investigated
    #ideally they would be dropped from sample space or rectified
    if not os.path.exists(lzma_db_filename):
        continue
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
    conn.loadextension(os.path.expanduser("~/libsqlitefunctions.so"))
    #build a list of parameters to supply to the query
    main_analysis_params = []
    minutes = range(1,61,1) + [120,24*60] + range(-45,0,1)
    for i in times.stamp:
        for m in minutes:
            main_analysis_params.append(
                    {"trademinstart":i,
                     "duration" : 60*m,
                     "sym_root" : stock,
                     "count" : 200 #No. of comparison days
                     })    
    #add the placebo parameters
    placebo_analysis_params=[]
#    for i in placebo_tests.unixtime[placebo_tests.TICKER==stock]:
#        for m in [60]:
#            placebo_analysis_params.append(
#                    {"trademinstart":i,
#                     "duration" : 60*m,
#                     "sym_root" : stock,
#                     "count" : 200 #No. of comparison days
#                     })
#    #combine
    PARAMS = main_analysis_params+placebo_analysis_params
    #get a cursor
    c =conn.cursor()
    print "Starting analysis"
    alldata = []
    print "Starting",stock
    rawdata = c.executemany(MAINQUERY, PARAMS).fetchall()
    #make the data long
    #SQLite has bad optimizations for making long, this will be much faster
    alldata=process_rawdata(rawdata)
    #output
    alldata.to_csv(analysis_filename,index=False)
    print "Done! Grabbing next stock"
    
print "Nothing left to do! Exiting..."
