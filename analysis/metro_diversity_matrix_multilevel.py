
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

shrinking_metros = OrderedDict()
qry = '''
SELECT B.cbsa_geoid10, A.CBSA, SUM(B.pop90), SUM(C.pop10)
FROM nhgis_cbsa_data AS A
JOIN subset_places_90_00 AS B
ON A.GISJOIN = "G"||B.cbsa_geoid10
JOIN subset_places_00_10 AS C
ON A.GISJOIN = "G"||C.cbsa_geoid10
GROUP BY A.CBSA
HAVING SUM(B.pop90) > SUM(C.pop10)
;
'''
results = cur.execute(qry)
for row in results:
	shrinking_metros[row[0]] = {'PC': {}, 'NPC': {} }

'''
iterate through metros, select sample places,
find principal cities, calc agg change in pop and 
diversity for principal cities and non-principal places
'''
for k, v in shrinking_metros.iteritems():
	print "+" * 60
	print k, v
	qry = '''
	SELECT B.nhgisplace10,
	A.pop90, A.white90, A.black90, A.asian90, A.hisp90, A.other90,
	B.pop10, B.white10, B.black10, B.asian10, B.hisp10, B.other10,	
	CASE 
		WHEN C.place_fips IS NOT NULL THEN 1
		ELSE 0
	END princip
	FROM subset_places_80_90 AS A
	JOIN subset_places_00_10 AS B
		ON A.nhgisplace90 = B.nhgisplace10 --requires places exist in each decade from 1980 to 2010 (too strict?)--
	LEFT JOIN principal_cities_2009 AS C 
		ON A.nhgisplace90 = "G" || C.state_fips || "0" || C.place_fips || "0"
	WHERE A.cbsa_geoid10 = '{}';
	'''.format(k)
	temp = pd.read_sql(qry, con, index_col='nhgisplace10')
	#################################################################
	# calc overall metro diversity
	for y in ['90', '10']:
		cols = ['white{}'.format(y), 'black{}'.format(y), 'asian{}'.format(y), 'hisp{}'.format(y), 'other{}'.format(y)]	
		metro_sum = pd.DataFrame(temp[cols].sum(axis=0))
		metro_sum.columns = ['count']
		# calc share of total in each category
		metro_sum['share'] = metro_sum['count'] * 1.0 / metro_sum['count'].sum(axis=0)
		# calc entropy
		Em = 0
		for i, x in metro_sum.iterrows():
			Em += x['share'] * np.log(1.0/x['share'])
		# scale measure
		Em = Em / np.log(len(cols))
		# print "metro scaled entropy: {}".format(round(Em,3))

		shrinking_metros[k]['PC']['Em{}'.format(y)] = Em
		shrinking_metros[k]['NPC']['Em{}'.format(y)] = Em
	#################################################################
	# aggregate PC and non-PC and calculate pop change and diversity and diversity change
	agg_data = temp.groupby('princip').sum()
	#################################################################
	# calc diversity in each period
	for y in ['90', '10']:
		cols = ['white{}'.format(y), 'black{}'.format(y), 'asian{}'.format(y), 'hisp{}'.format(y), 'other{}'.format(y)]
		for c in cols:
			agg_data['p{}'.format(c)] = agg_data['{}'.format(c)] * 1.0 / agg_data[cols].sum(axis=1)

		agg_data['E{}'.format(y)] = 0
		for c in cols:
			agg_data.loc[agg_data['p{}'.format(c)] > 0.0, 'E{}'.format(y)] += agg_data['p{}'.format(c)] * np.log(1.0/agg_data['p{}'.format(c)])
		agg_data['E{}'.format(y)] = agg_data['E{}'.format(y)] / np.log(len(cols))

	# print agg_data[['E90', 'E10']]
	shrinking_metros[k]['PC']['E90'] = agg_data.loc[1]['E90']
	shrinking_metros[k]['PC']['E10'] = agg_data.loc[1]['E10']
	shrinking_metros[k]['NPC']['E90'] = agg_data.loc[0]['E90']
	shrinking_metros[k]['NPC']['E10'] = agg_data.loc[0]['E10']
	#################################################################
	# calc pop change in each group
	agg_data['pop_chg'] = (agg_data['pop10'] - agg_data['pop90']) * 1.0 / agg_data['pop90'] * 100
	# print agg_data[['pop_chg']]
	shrinking_metros[k]['PC']['pop_chg'] = agg_data.loc[1]['pop_chg']
	shrinking_metros[k]['NPC']['pop_chg'] = agg_data.loc[0]['pop_chg']
	#################################################################


con.close()
df = pd.DataFrame.from_dict({(i,j): shrinking_metros[i][j] 
							for i in shrinking_metros.keys() 
							for j in shrinking_metros[i].keys()},
							orient='index')

df['div_diff'] = df['E10'] - df['E90']

df['div_diff_binary'] = 'MD' # more diverse
df.loc[df['div_diff'] < 0, 'div_diff_binary'] = 'LD' # less diverse

# # calc relative diff in each period
df['div_diff_2']
df['']


df['pop_chg_binary'] = 'G' # pop growth
df.loc[df['pop_chg'] < 0, 'pop_chg_binary'] = 'L' # pop loss 

print df.head()

df = df.reset_index()
print pd.crosstab(index=df['level_1'], columns=[df['pop_chg_binary'], df['div_diff_binary']])

df.to_csv('loss_metro_data.csv')
#########################################################################