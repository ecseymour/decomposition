from pysqlite2 import dbapi2 as sql
import pandas as pd
import numpy as np
from collections import OrderedDict

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

'''
for each MSA,
for each decade (1980-2010),
calculate city/suburb agg. share
for each race/ethnic group, diversity, 
and pop. change
'''
######################################################
# select shrinking metros
shrinking_metros = OrderedDict()
qry = '''
SELECT A.cbsa_geoid10, A.name10, 
SUM(pop80), SUM(pop90), SUM(pop00), SUM(pop10), COUNT(*) AS places
FROM subset_places_00_10 AS A
JOIN subset_places_80_90 AS B
ON A.nhgisplace10 = B.nhgisplace90
GROUP BY A.cbsa_geoid10
HAVING SUM(pop00) > SUM(pop10) AND SUM(pop80) > SUM(pop10)
;
'''
results = cur.execute(qry)
for row in results:
	shrinking_metros[row[0]] = row[1]
######################################################
# find agg city/suburb data for each shrinking metro
count = 0
for k, v in shrinking_metros.iteritems():
	print '+' * 60 
	print k, v, count
	qry = '''
	SELECT A.nhgisplace10, A.name10,
	B.white00, B.black00, B.asian00, B.hisp00, B.other00, 
	A.white10, A.black10, A.asian10, A.hisp10, A.other10,
	A.pop10, A.pop00,	
	CASE 
		WHEN A.pop00 > A.pop10 THEN 'loss'
		ELSE 'growth'
	END loss_flag
	FROM subset_places_00_10 AS A JOIN subset_places_90_00 AS B
		ON A.nhgisplace10 = B.nhgisplace00
	WHERE A.cbsa_geoid10 = '{}'
	;
	'''.format(k)
	temp = pd.read_sql(qry, con, index_col='nhgisplace10')
	temp = temp.groupby(['name10','loss_flag']).sum()

	# calc each groups share of total population and diversity
	lst = ['00', '10']
	for l in lst:
		cols = ['white{}'.format(l), 'black{}'.format(l), 'asian{}'.format(l), 'hisp{}'.format(l), 'other{}'.format(l)]
		for c in cols:
			temp['p{}'.format(c)] = temp['{}'.format(c)] * 1.0 / temp[cols].sum(axis=1)
		# calc diversity for aggregated growth/loss CBSAs
		temp['E{}'.format(l)] = 0
		for c in cols:
			temp.loc[temp['p{}'.format(c)] > 0.0, 'E{}'.format(l)] += temp['p{}'.format(c)] * np.log(1.0/temp['p{}'.format(c)])
		temp['E{}'.format(l)] = temp['E{}'.format(l)] / np.log(len(cols))

	cols = ['pwhite00', 'pblack00', 'pasian00', 'phisp00', 'pother00',
			'pwhite10', 'pblack10', 'pasian10', 'phisp10', 'pother10', 
			'pop00', 'pop10', 'E00', 'E10']
	print temp[cols].round(2)

	temp = temp[cols].round(2)

	if count==0:
		final_df = temp
	elif count >= 1:
		final_df = pd.concat([final_df, temp])
	print final_df
	count+=1


outf = "/home/eric/Documents/franklin/fowler/loss_metros_places_grouped_by_loss_1990-2010.xlsx"
writer = pd.ExcelWriter(outf) 
final_df.to_excel(writer, sheet_name="data")


con.close()