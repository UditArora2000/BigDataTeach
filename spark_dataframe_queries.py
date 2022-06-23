q1_a = """
    select status, date as date_n, count(distinct pid) as count_n
	from pullreq
	where status='opened'
	group by status, date_n
	order by date_n"""

q1_b = """
    select status, date, count(distinct pid) as count_n
	from pullreq
	where status='discussed'
	group by status, date
	order by date"""

month_com = """
	select name, count(*) as count_n, date_part('month', date) as month 
	from pullreq
	where status='discussed'
	group by month, name"""

q2 = """
	select month, count_n, name from month_com as M 
	where count_n = (select max(count_n) 
					 from month_com as N 
					 where M.month = N.month)
	order by month"""

week_com = """ 
	select name, count(*) as count_n, date_part('year', date) as year, 
	date_part('week', date) as week 
	from pullreq where status='discussed' group by year, week, name"""

q3 = """
	select year, week, name, count_n
	from week_com as M 
	where count_n = (select max(count_n) 
				     from week_com as N 
				     where M.year=N.year and M.week=N.week)
	order by year, week"""

q4 = """
	select date_part('year', date) as year, 
	date_part('week', date) as week, count(distinct pid) as count_n
	from pullreq
	where status='opened'
	group by year, week
	order by year, week"""

q5 = """
    select date_part('month', date) as month, 
    count(distinct pid) as count_n
	from pullreq
	where status = 'merged'
	and date_part('year', date) = 2010
	group by month
	order by month"""

q6 = """
    select date, count(*) from pullreq
	group by date
	order by date"""

q7 = """
	select name, count(distinct pid) as count_n from pullreq
	where status = 'opened' and 
	date_part('year', date) = 2011
	group by name
	order by count(distinct pid) desc
	limit 1"""