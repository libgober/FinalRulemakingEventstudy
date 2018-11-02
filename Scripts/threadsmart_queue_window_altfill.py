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
--1474646400,V
WITH RECURSIVE times(t,dt) AS (
  SELECT 
  	time(datetime(:eventtime-60*:minbefore,'unixepoch')),
  	-:minbefore
  UNION ALL
  SELECT time(t,'+1 minute'),
  	dt+1
  	from times
   WHERE dt < :minafter
)
/*select * from times order by dt asc limit 3;
/*t        |dt  |
---------|----|
15:00:00 |-60 |
15:01:00 |-59 |
15:02:00 |-58 |*/
,dates as (select *,
	row_number() over(order by caldate desc) -1 as dayno
	from 
	(select date(time,'unixepoch') as caldate from mp where time<=:eventtime group by caldate order by caldate desc limit :comparison_days + 1 ))
,intended_obs as (select *,strftime('%s',caldate || ' ' || t)  as intended_unixtime from dates,times)
/*select * from intended_obs order by dt*dt asc limit 3;
caldate    |dayno |t        |dt |intended_unixtime |
-----------|------|---------|---|------------------|
2016-09-23 |0     |16:00:00 |0  |1474646400        |
2016-09-22 |1     |16:00:00 |0  |1474560000        |
2016-09-21 |2     |16:00:00 |0  |1474473600        |*/
,full_block as (
--simple idea is to cross join and filter to only results we want
--in particular, if the intended time is after the event, we want to get the min time after the intended
-- if the intended time is before the event, we want to get the max time before the intended
select *,
	datetime(time,'unixepoch') as actual_time from intended_obs,mp
	where dt>=0 and (time = (select min(time) from mp where time>=intended_unixtime))
union all 
select *,
	datetime(time,'unixepoch') as actual_time from intended_obs,mp
	where dt<0 and (time = (select max(time) from mp where time<=intended_unixtime))
)
/*select * from full_block;
caldate    |dayno |t        |dt |intended_unixtime |time       |quote             |PATH_SP500_EW         |PATH_TOTALMARKET     |actual_time         |
-----------|------|---------|---|------------------|-----------|------------------|----------------------|---------------------|--------------------|
2016-09-23 |0     |16:00:00 |0  |1474646400        |1474646400 |82.56387779100001 |3.796283260206089     |4.439391164103219    |2016-09-23 16:00:00 |
2016-09-22 |1     |16:00:00 |0  |1474560000        |1474560000 |83.348601227      |4.289473695641045     |4.506919675927066    |2016-09-22 16:00:00 |
2016-09-21 |2     |16:00:00 |0  |1474473600        |1474473600 |83.174752809      |4.495345771595482     |5.190538199092525    |2016-09-21 16:00:00 |
*/
,diffs as (
select 
	A.quote-B.quote as returns_simple,
	A.PATH_SP500_EW-B.PATH_SP500_EW as returns_sp500ew,
	A.PATH_TOTALMARKET - B.PATH_TOTALMARKET as returns_totalmkt,
	A.caldate,
	A.dayno,
	A.t,
	A.dt,
	A.time,
	A.actual_time,
	A.intended_unixtime,
	B.quote as reference_simple,
	B.PATH_SP500_EW as reference_spt500ew,
	B.PATH_TOTALMARKET as reference_totalmkt,
	A.quote as quote,
	A.PATH_SP500_EW as sp500ew,
	A.PATH_TOTALMARKET as totalmkt
	from (
	select * from full_block where dt!=0) A
	left join (select * from full_block where dt=0) B
	on A.dayno=B.dayno)
/*select * from full_block where dt!=0) A
	left join (select * from full_block where dt=0) B
	on A.dayno=B.dayno limit 3;
r0                  |r1                   |caldate    |dayno |t        |dt  |intended_unixtime |time       |quote             |PATH_SP500_EW     |PATH_TOTALMARKET  |actual_time         |caldate:1  |dayno:1 |t:1      |dt:1 |intended_unixtime:1 |time:1     |quote:1           |PATH_SP500_EW:1   |PATH_TOTALMARKET:1 |actual_time:1       |
--------------------|---------------------|-----------|------|---------|----|------------------|-----------|------------------|------------------|------------------|--------------------|-----------|--------|---------|-----|--------------------|-----------|------------------|------------------|-------------------|--------------------|
0.4222327690000185  |0.17449444457367802  |2016-09-26 |0     |08:35:00 |-60 |1474878900        |1474646400 |82.56387779100001 |3.796283260206089 |4.439391164103219 |2016-09-23 16:00:00 |2016-09-26 |0       |09:35:00 |0    |1474882500          |1474882500 |82.14164502199999 |3.621788815632411 |4.6595362825169735 |2016-09-26 09:35:00 |
0.10463063900000691 |0.035454588118756014 |2016-09-23 |1     |08:35:00 |-60 |1474619700        |1474560000 |83.348601227      |4.289473695641045 |4.506919675927066 |2016-09-22 16:00:00 |2016-09-23 |1       |09:35:00 |0    |1474623300          |1474623300 |83.243970588      |4.254019107522289 |4.613251978902988  |2016-09-23 09:35:00 |
-0.4253768210000004 |0.03453759774477572  |2016-09-22 |2     |08:35:00 |-60 |1474533300        |1474473600 |83.174752809      |4.495345771595482 |5.190538199092525 |2016-09-21 16:00:00 |2016-09-22 |2       |09:35:00 |0    |1474536900          |1474536900 |83.60012963       |4.460808173850706 |4.709599840887276  |2016-09-22 09:35:00 |*/
/*select * from diffs;
returns_simple      |returns_sp500ew      |returs_totalmkt      |caldate    |dayno |t        |dt  |time       |actual_time         |intended_unixtime |
--------------------|---------------------|---------------------|-----------|------|---------|----|-----------|--------------------|------------------|
0.4222327690000185  |0.17449444457367802  |-0.22014511841375484 |2016-09-26 |0     |08:35:00 |-60 |1474646400 |2016-09-23 16:00:00 |1474878900        |
0.10463063900000691 |0.035454588118756014 |-0.10633230297592178 |2016-09-23 |1     |08:35:00 |-60 |1474560000 |2016-09-22 16:00:00 |1474619700        |
-0.4253768210000004 |0.03453759774477572  |0.48093835820524866  |2016-09-22 |2     |08:35:00 |-60 |1474473600 |2016-09-21 16:00:00 |1474533300        |*/
,percentiles as (select 
			(
				percent_rank() 
				over 
				(partition by dt order by returns_simple) 
				+ 
				1 - percent_rank() 
				over (partition by dt order by returns_simple desc)
			)/2 as ranked_returns_simple,
			(
				percent_rank() 
				over 
				(partition by dt order by returns_sp500ew) 
				+
				1- percent_rank() 
				over 
				(partition by dt order by returns_sp500ew desc)
			)/2 as ranked_returns_sp500ew,
			(
				percent_rank() 
				over 
				(partition by dt order by returns_totalmkt)
				+ 1 - percent_rank() 
				over 
				(partition by dt order by returns_totalmkt desc)
			)/2 as ranked_returns_totalmkt,
			dt,dayno
			from diffs)
/*select * from percentiles where dayno=0 and dt=60;
ranked_returns_simple |ranked_returns_sp500ew |ranked_returns_totalmkt |dt |dayno |
----------------------|-----------------------|------------------------|---|------|
0.12999999999999995   |0.14000000000000007    |0.81                    |60 |0     |
*/
,moments as (select dt,
	avg(returns_simple) as m1,
	sum(returns_simple is not NULL) as n1,
	stdev(returns_simple) as s1,
	avg(returns_sp500ew) as m2,
	stdev(returns_sp500ew) as s2,
	sum(returns_sp500ew is not NULL) as n2,
	avg(returns_totalmkt) as m3,
	stdev(returns_totalmkt) as s3,
	sum(returns_totalmkt is not NULL) as n3
	from diffs where dayno!=0
group by dt)
/*select * from moments;
dt  |m1                    |n1  |s1                   |m2                     |s2                     |n2  |m3                     |s3                     |n3  |
----|----------------------|----|---------------------|-----------------------|-----------------------|----|-----------------------|-----------------------|----|
-60 |0.013126886249999643  |100 |0.17383780635914792  |0.019861559242575627   |0.1199259970900596     |100 |0.023902453221530777   |0.1336392266436155     |100 |
-59 |0.013905479809999833  |100 |0.173110964022661    |0.019811427133073913   |0.11919713439046806    |100 |0.02287404296058261    |0.13483465010406598    |100 |
-58 |0.01736740573000063   |100 |0.17135982170376257  |0.02168913152881749    |0.11805683919254448    |100 |0.02285280580966866    |0.1367065033972689     |100 |*/
,busy_results as (select * from (select * from diffs where dayno=0) A 
left join (select * from percentiles where dayno=0) B
on A.dt=B.dt
left join moments on B.dt=moments.dt)
/*select * from busy_results;
returns_simple       |returns_sp500ew        |returns_totalmkt       |caldate    |dayno |t        |dt  |time       |actual_time         |intended_unixtime |reference_simple  |reference_spt500ew |reference_totalmkt |quote             |sp500ew            |totalmkt           |ranked_returns_simple |ranked_returns_sp500ew |ranked_returns_totalmkt |dt:1 |dayno:1 |dt:2 |m1                    |n1  |s1                   |m2                     |s2                     |n2  |m3                     |s3                     |n3  |
---------------------|-----------------------|-----------------------|-----------|------|---------|----|-----------|--------------------|------------------|------------------|-------------------|-------------------|------------------|-------------------|-------------------|----------------------|-----------------------|------------------------|-----|--------|-----|----------------------|----|---------------------|-----------------------|-----------------------|----|-----------------------|-----------------------|----|
-0.4222327690000185  |-0.17449444457367802   |0.22014511841375484    |2016-09-23 |0     |16:01:00 |1   |1474882500 |2016-09-26 09:35:00 |1474646460        |82.56387779100001 |3.796283260206089  |4.439391164103219  |82.14164502199999 |3.621788815632411  |4.6595362825169735 |0.12999999999999995   |0.14000000000000007    |0.81                    |1    |0       |1    |0.04686145404000044   |100 |0.5547141336331209   |0.03641278599463993    |0.31379651935116826    |100 |0.024888753272871603   |0.2633106196826183     |100 |
-0.4222327690000185  |-0.17449444457367802   |0.22014511841375484    |2016-09-23 |0     |16:02:00 |2   |1474882500 |2016-09-26 09:35:00 |1474646520        |82.56387779100001 |3.796283260206089  |4.439391164103219  |82.14164502199999 |3.621788815632411  |4.6595362825169735 |0.12999999999999995   |0.14000000000000007    |0.81                    |2    |0       |2    |0.04686145404000044   |100 |0.5547141336331209   |0.03641278599463993    |0.31379651935116826    |100 |0.024888753272871603   |0.2633106196826183     |100 |
-0.4222327690000185  |-0.17449444457367802   |0.22014511841375484    |2016-09-23 |0     |16:03:00 |3   |1474882500 |2016-09-26 09:35:00 |1474646580        |82.56387779100001 |3.796283260206089  |4.439391164103219  |82.14164502199999 |3.621788815632411  |4.6595362825169735 |0.12999999999999995   |0.14000000000000007    |0.81                    |3    |0       |3    |0.04686145404000044   |100 |0.5547141336331209   |0.03641278599463993    |0.31379651935116826    |100 |0.024888753272871603   |0.2633106196826183     |100 |*/
select 
	:sym_root,
	 date(:eventtime,'unixepoch') as eventdate,
     time(:eventtime,'unixepoch') as eventtime,
     :eventtime as event_unixtime,
     dt,
     intended_unixtime,
     time as actual_unixtime,
     actual_time as actual_human_time,
     ranked_returns_simple as q1,
     n1,
     m1,
     s1,
     returns_simple as r1,
     (returns_simple - m1)/s1 as t1,
     ranked_returns_sp500ew  as q2,
     n2,
     m2,
     s2,
     returns_sp500ew as r2,
     (returns_sp500ew - m2)/s2 as t2,
     ranked_returns_totalmkt as q3,
     n3,
     m3,
     s3,
     returns_totalmkt as r3,
     (returns_totalmkt - m3)/s3 as t3
  from 
  busy_results;
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
    common = pd.DataFrame(rawdata).iloc[:,0:8]
    common.columns = ["sym_root",
                      "eventdate",
                      "eventtime",
                      "t0_unixtime",
                      'duration',
                      "intended_t1_unixtime",
                      "actual_t1_unixtime",
                      "actual_t1_humantime"
                      ]
    #first tmp is the simple returns
    simple=pd.DataFrame(rawdata).iloc[:,8:14]
    simple.columns = ['q','n','m','s','r','t']
    simple = pd.concat([common,simple],axis=1)
    simple["market_model"] = "None"
    #
    rsp=pd.DataFrame(rawdata).iloc[:,14:20]
    rsp.columns = ['q','n','m','s','r','t']
    rsp = pd.concat([common,rsp],axis=1)
    rsp["market_model"] = 'RSP'
    #vti
    vti = pd.DataFrame(rawdata).iloc[:,20:26]
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
    for i in times.stamp:
        main_analysis_params.append(
                {"eventtime":i,
                 "minbefore" : 90,
                 "minafter" : 1380,
                 "sym_root" : stock,
                 "comparison_days" : 200 #No. of comparison days
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
    rawdata = c.executemany(MAINQUERY,PARAMS).fetchall()
    #make the data long
    #SQLite has bad optimizations for making long, this will be much faster
    alldata=process_rawdata(rawdata)
    #output
    alldata.to_csv(analysis_filename,index=False)
    print "Done! Grabbing next stock"
    
print "Nothing left to do! Exiting..."
