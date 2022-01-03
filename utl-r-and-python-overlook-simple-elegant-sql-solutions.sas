%let pgm=utl-r-and-python-overlook-simple-elegant-sql-solutions;

R and Python overlook simple elegant SQL solutions

github
https://tinyurl.com/2wad37aj
https://github.com/rogerjdeangelis/utl-r-and-python-overlook-simple-elegant-sql-solutions

MACROS
https://tinyurl.com/58pp9nz6
https://github.com/rogerjdeangelis/utl-macros-used-in-many-of-rogerjdeangelis-repositories


Find the month of the first missing and last missing value by age

     Two Solutions
         1. SAS  (there is SAS datastep solution more readable than R or Python (DOW) )
         2. R
         2. Python

As a side note normalizing opens up the problem to simpler cleaner solutions.
especially SQL.
It eliminates looping;

It seems to me that sql code in both R and Python provides a much more understandable
solution to many problems, rather than non SQL in R or Python.

Here is an sample base r solution

/*___
|  _ \
| |_) |
|  _ <
|_| \_\

*/

* This seems a bit much dcast(melt?);

library(data.table)
dcast(melt(setDT(df), id.var = c('year', 'country'), na.rm = TRUE)[,
    .(First = min(year, na.rm = TRUE), Last = max(year, na.rm = TRUE)),
   .(country, variable)], country ~variable, value.var = c("First", "Last"), sep=".")

library(tidyverse)

# Put the data in a tidy format
gathered_df <- df %>%
  gather(key = series_no, value = val, series1:series2, na.rm = TRUE)

# find the first and last by country and series
sum_df <- gathered_df %>%
  group_by(series_no, country) %>%
  summarise(Last = max(year),
            First = min(year))

# make min and max into a column, then add a label
# Eg First:series2
reduced_df <- sum_df %>%
  gather(key = measurement, value = year, First:Last) %>%
  mutate(label = factor(paste(series_no, ":", measurement))) %>%
  group_by(label) %>%
  select(label, year, country)

# Put the output in a table format as you wanted
output <- reduced_df %>%
  spread(key = label, value = year)

/*         _             _       _   _
 ___  __ _| |  ___  ___ | |_   _| |_(_) ___  _ __
/ __|/ _` | | / __|/ _ \| | | | | __| |/ _ \| `_ \
\__ \ (_| | | \__ \ (_) | | |_| | |_| | (_) | | | |
|___/\__, |_| |___/\___/|_|\__,_|\__|_|\___/|_| |_|
        |_|
*/
Here is ths SQL solution

proc sql;
  select
      age
     ,min(month) as mthMisStart
     ,max(month) as mthMisEnd
  from
      sd1.have
  where
      missing(score)
  group
      by age
;quit;

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;

libname sd1 "d:/sd1";

data sd1.have;
 retain age 32 month 0;
 infile cards line=lyn;
 input score @@;
 month=month+1;
 score=int(50*uniform(1234) +50) + score;
 if lyn=2 then do; age=int(30*uniform(1234) +18); month=1; end;
cards4;
1 2 3 4 5 . . . .
1 . . 4 5 6 7 . 9
1 2 3 . . . 7 8 9
;;;;
run;quit;

Up to 40 obs WORK.HAVE total obs=27 02JAN2022:17:28:38

Obs    AGE    MONTH    SCORE

  1     32      1        63
  2     32      2        56
  3     32      3        72
  4     32      4        58
  5     32      5        67
  6     32      6         .
  7     32      7         .
  8     32      8         .
  9     32      9         .

 10     19      1        58
 11     19      2         .   min is month 2
 12     19      3         .
 13     19      4        99
 14     19      5       103
 15     19      6        92
 16     19      7        77
 17     19      8         .   max is month  8
 18     19      9        75

 19     26      1        76
 20     26      2        69
 21     26      3        65
 22     26      4         .
 23     26      5         .
 24     26      6         .
 25     26      7        57
 26     26      8        83
 27     26      9       104

/*           _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
*/

Up to 40 obs from WANT_SAS total obs=3 03JAN2022:06:38:25

Obs    AGE    MTHMISSTART    MTHMISEND

 1      19         2             8
 2      26         4             6
 3      32         6             9

/*
 _ __  _ __ ___   ___ ___  ___ ___  ___  ___
| `_ \| `__/ _ \ / __/ _ \/ __/ __|/ _ \/ __|
| |_) | | | (_) | (_|  __/\__ \__ \  __/\__ \
| .__/|_|  \___/ \___\___||___/___/\___||___/
|_|
 ___  __ _ ___
/ __|/ _` / __|
\__ \ (_| \__ \
|___/\__,_|___/

*/

proc sql;
  create
      table want_sas as
  select
      age
     ,min(month) as mthMisStart
     ,max(month) as mthMisEnd
  from
      sd1.have
  where
      missing(score)
  group
      by age
;quit;

/*___
|  _ \
| |_) |
|  _ <
|_| \_\

*/

* Note solution handles long variable name in sas v5 transport file;

%utlfkil(d:/xpt/want_r.xpt);

%utl_submit_r64("
  library(haven);
  library(SASxport);
  library(sqldf);
  have<-read_sas('d:/sd1/have.sas7bdat');
  have;
  want_r<-sqldf('
     select
         AGE
        ,min(MONTH) as mthMisStart
        ,max(MONTH) as mthMisEnd
     from
         have
     where
         SCORE is null
     group
         by AGE
  ');

  label(want_r$mthMisStart) <-'mthMisStart';
  label(want_r$mthMisEnd ) <-'mthMisEnd';

  str(want_r);

  write.xport(want_r,file='d:/xpt/want_r.xpt');

  ");

proc datasets lib=work mt=view mt=data;
    delete want_r ;
run;quit;

libname xpt xport "d:/xpt/want_r.xpt";

proc contents data=xpt._all_;
run;quit;

proc print data=xpt.want_r;
run;quit;

data want_from_r;  /* do not use want_r */

  %utl_rens(xpt.want_r);
  set want_r;

run;quit;

* note long variable names;

Up to 40 obs WORK.WANT_FROM_R total obs=3 03JAN2022:07:39:09

Obs    AGE    MTHMISSTART    MTHMISEND

 1      19         2             8
 2      26         4             6
 3      32         6             9

/*           _   _
 _ __  _   _| |_| |__   ___  _ __
| `_ \| | | | __| `_ \ / _ \| `_ \
| |_) | |_| | |_| | | | (_) | | | |
| .__/ \__, |\__|_| |_|\___/|_| |_|
|_|    |___/
*/

* is does not look like python can populate the xpt file with a SAS 40 byte label;

libname tmp "c:/temp";
data tmp.have;
 retain age 32 month 0;
 infile cards line=lyn;
 input score @@;
 month=month+1;
 score=int(50*uniform(1234) +50) + score;
 if lyn=2 then do; age=int(30*uniform(1234) +18); month=1; end;
cards4;
1 2 3 4 5 . . . .
1 . . 4 5 6 7 . 9
1 2 3 . . . 7 8 9
;;;;
run;quit;

proc datasets lib=work kill;
run;quit;

%utlfkil(c:/temp/want_r.xpt);

%utl_pybegin39;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
import pandas as pd
from pandasql import sqldf
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
have, meta = pyreadstat.read_sas7bdat("c:/temp/have.sas7bdat")
print(have);
res = pdsql("""
     select
         AGE
        ,min(MONTH) as mthStart
        ,max(MONTH) as mthEnd
     from
         have
     where
         SCORE is null
     group
         by AGE
  """);
print(res);
ds = xport.Dataset(res, name='want_r')
with open('c:/temp/want_pydat.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend39;

libname pyxpt xport "c:/temp/want_pydat.xpt";

proc contents data=pyxpt._all_;
run;quit;

proc print data=pyxpt.want_r;
run;quit;

data want_py;
   set pyxpt.want_r;
run;quit;

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
