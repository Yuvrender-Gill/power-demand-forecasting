proc import datafile = 'N:\Desktop\demand_data.csv'
out = power_data
dbms = CSV
replace;
run;

data power_data;
set power_data;
year = year(date);
month = month(date);
day = day(date);
week = week(date);
weekday = weekday(date);
where year(date) > 2003;
run;

data power_data;
set power_data;
weekend = 0;
workday = 0;
if weekday = 1 or weekday = 7 then weekend = 1;
if weekday in (2,3,4,5,6) then workday = 1;
run;


data power_data_hourly;
set power_data;
run;

proc export data = power_data_hourly
outfile = "N:\Desktop\new\power_data_hourly.csv"
dbms = CSV
replace;
run;

%MACRO aggregate_daily(name, fun);

proc sql;
create table power_data_daily_&name as 
select distinct date, year, month, day, week,  weekday, weekend, workday,
&fun.(Ontario_demand) as Ontario_demand, &fun.(northwest) as northwest, &fun.(northeast) as northeast, &fun.(ottawa) as ottawa,
&fun.(east) as east, &fun.(toronto) as toronto, &fun.(essa) as essa, &fun.(bruce) as bruce, &fun.(southwest) as southwest,
&fun.(niagara) as niagara, &fun.(west) as west
from power_data
group by date;
quit;

proc export data = power_data_daily_&name
outfile = "N:\Desktop\new\power_data_daily_&name..csv"
dbms = CSV
replace;
run;

%mend aggregate_daily;

%aggregate_daily(total, sum);
%aggregate_daily(average, mean);
%aggregate_daily(max, max);
%aggregate_daily(min, min);

%MACRO aggregate_weekly(name, fun);

proc sql;
create table power_data_weekly_&name as 
select distinct year, week, 
&fun.(Ontario_demand) as Ontario_demand, &fun.(northwest) as northwest, &fun.(northeast) as northeast, &fun.(ottawa) as ottawa,
&fun.(east) as east, &fun.(toronto) as toronto, &fun.(essa) as essa, &fun.(bruce) as bruce, &fun.(southwest) as southwest,
&fun.(niagara) as niagara, &fun.(west) as west
from power_data
group by year, week;
quit;

proc export data = power_data_weekly_&name
outfile = "N:\Desktop\new\power_data_weekly_&name..csv"
dbms = CSV
replace;
run;

%mend aggregate_weekly;

%aggregate_weekly(total, sum);
%aggregate_weekly(average, mean);
%aggregate_weekly(max, max);
%aggregate_weekly(min, min);

%MACRO aggregate_monthly(name, fun);

proc sql;
create table power_data_monthly_&name as 
select distinct year, month, 
&fun.(Ontario_demand) as Ontario_demand, &fun.(northwest) as northwest, &fun.(northeast) as northeast, &fun.(ottawa) as ottawa,
&fun.(east) as east, &fun.(toronto) as toronto, &fun.(essa) as essa, &fun.(bruce) as bruce, &fun.(southwest) as southwest,
&fun.(niagara) as niagara, &fun.(west) as west
from power_data
group by year, month;
quit;

proc export data = power_data_monthly_&name
outfile = "N:\Desktop\new\power_data_monthly_&name..csv"
dbms = CSV
replace;
run;

%mend aggregate_monthly;

%aggregate_monthly(total, sum);
%aggregate_monthly(average, mean);
%aggregate_monthly(max, max);
%aggregate_monthly(min, min);


%MACRO aggregate_yearly(name, fun);

proc sql;
create table power_data_yearly_&name as 
select distinct year, 
&fun.(Ontario_demand) as Ontario_demand, &fun.(northwest) as northwest, &fun.(northeast) as northeast, &fun.(ottawa) as ottawa,
&fun.(east) as east, &fun.(toronto) as toronto, &fun.(essa) as essa, &fun.(bruce) as bruce, &fun.(southwest) as southwest,
&fun.(niagara) as niagara, &fun.(west) as west
from power_data
group by year;
quit;

proc export data = power_data_yearly_&name
outfile = "N:\Desktop\new\power_data_yearly_&name..csv"
dbms = CSV
replace;
run;

%mend aggregate_yearly;

%aggregate_yearly(total, sum);
%aggregate_yearly(average, mean);
%aggregate_yearly(max, max);
%aggregate_yearly(min, min);
