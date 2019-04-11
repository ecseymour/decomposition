'''
get share each race/ethnicity group
in Milwaukee for each decade
'''
import sqlite3 as sql
import pandas as pd

db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
cur = con.cursor()

periods = [ ['80', '90'], ['90', '00'], ['00', '10'] ]

for p in periods:
	# print "+" * 50
	# print p
	start = p[0]
	end = p[1]
	# collect places data
	qry = '''
	SELECT '{}' as year,
	pop{} * 1.0 / 100 AS pop, 
	pwhite{} AS pwhite, 
	pblack{} as pblack, 
	pasian{} as pasian, 
	phisp{} as phisp, 
	pother{} as pother,
	Ep
	FROM subset_places_{}_{} 
	WHERE nhgisplace{} = 'G550530000';
	'''.format(end, end, end, end, end, end, end, start, end, end)
	# print qry
	df = pd.read_sql(qry, con)
	# print df

	if end=='90':
		df_final = df
	else:
		df_final = df_final.append(df)

df_final.index = df_final['year']
df_final = df_final[df_final.columns[1:]]

print (df_final * 100).round(1)

con.close()