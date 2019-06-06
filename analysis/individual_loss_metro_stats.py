from pysqlite2 import dbapi2 as sql
import pandas as pd
import numpy as np
from collections import OrderedDict

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
# con.enable_load_extension(True)
# con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

'''
for each MSA,
for each decade (2000-2010);
group into largest principal cit,
other principal cities,
and non-principal cities;
sum race/ethnicity categories,
calc. each group's share of tot. pop.,
and decomposition

many of these "loss-loss" metros only have one principal city,
so perhaps group them all together. 
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
-- LIMIT 1
;
'''
results = cur.execute(qry)
for row in results:
	shrinking_metros[row[0]] = row[1]
######################################################
count = 0
for k, v in shrinking_metros.iteritems():
	print '+' * 40
	print k, v
	######################################################
	qry = '''
	SELECT A.nhgisplace10, A.name10, A.place10,
	B.white00, B.black00, B.asian00, B.hisp00, B.other00, 
	A.white10, A.black10, A.asian10, A.hisp10, A.other10,
	A.pop10, A.pop00, A.Ep AS Ep10, B.Ep AS Ep00,
	CASE 
		WHEN C.place_fips IS NOT NULL THEN 'pc'
		ELSE 'npc'
	END princip,
	CASE 
		WHEN A.pop00 > A.pop10 THEN 'loss'
		ELSE 'growth'
	END loss_flag
	FROM subset_places_00_10 AS A JOIN subset_places_90_00 AS B
		ON A.nhgisplace10 = B.nhgisplace00
	LEFT JOIN principal_cities_2009 AS C
		ON A.nhgisplace10 = "G" || C.state_fips || "0" || C.place_fips || "0"
	WHERE A.cbsa_geoid10 = '{}'
	ORDER BY A.pop10 DESC;
	'''.format(k)
	######################################################
	# calc metro D in 2000 and 2010
	# ALSO CALC TOTAL BETWEEN PLACE H IN EACH MSA
	df = pd.read_sql(qry, con, index_col='nhgisplace10')
	lst = ['00', '10']
	Em00 = 0
	Em10 = 0
	for l in lst:
		cols = ['white{}'.format(l), 'black{}'.format(l), 'asian{}'.format(l), 'hisp{}'.format(l), 'other{}'.format(l)]
		total_msa = pd.DataFrame(df[cols].sum(axis=0))
		total_msa.columns = ['count']
		# calc share of total in each category
		total_msa['share'] = total_msa['count'] * 1.0 / total_msa['count'].sum(axis=0)
		# calc entropy
		if l=='00':
			for i, x in total_msa.iterrows():
				Em00 += x['share'] * np.log(1.0/x['share'])
			# scale measure
			Em00 = Em00 / np.log(len(cols))
			print "MSA scaled entropy 2000: {}".format(round(Em00,3))
		elif l=='10':
			for i, x in total_msa.iterrows():
				Em10 += x['share'] * np.log(1.0/x['share'])
			# scale measure
			Em10 = Em10 / np.log(len(cols))
			print "MSA scaled entropy 2010: {}".format(round(Em10,3))

	# compare each place to metro
	Hpm00 = 0
	Hpm10 = 0
	for l in lst:
		if l=='00':
			for i, x in df.iterrows():
				Hpm00 += x['pop{}'.format(l)] * (Em00 - x['Ep00'])
			Hpm00 = (1.0 / (df['pop00'].sum() * Em00) ) * Hpm00
			print "Hpm 2000: {}".format(round(Hpm00,3))
		elif l=='10':
			for i, x in df.iterrows():
				Hpm10 += x['pop{}'.format(l)] * (Em10 - x['Ep10'])
			Hpm10 = (1.0 / (df['pop10'].sum() * Em10) ) * Hpm10
			print "Hpm 2010: {}".format(round(Hpm10,3))
	######################################################
	# calc each groups share of total population and diversity
	temp = pd.read_sql(qry, con, index_col='nhgisplace10')
	temp = temp.groupby(['name10','princip', 'loss_flag']).sum()
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

	# add decomp
	temp['Hgm00'] = temp['pop00'] * (Em00 - temp['E00'])
	temp['Hgm00'] = (1.0 / (df['pop00'].sum() * Em00) ) * temp['Hgm00']
	temp['Hgm00pct'] = temp['Hgm00'] * 1.0 / Hpm00 * 100

	temp['Hgm10'] = temp['pop10'] * (Em10 - temp['E10'])
	temp['Hgm10'] = (1.0 / (df['pop10'].sum() * Em10) ) * temp['Hgm10']
	temp['Hgm10pct'] = temp['Hgm10'] * 1.0 / Hpm10 * 100

	temp['HgmChg'] = (temp['Hgm10'] - temp['Hgm00']) * 1.0 / np.abs(temp['Hgm00']) * 100

	temp['pop00pct'] = temp['pop00'] * 1.0 / df['pop00'].sum() * 100
	temp['pop10pct'] = temp['pop10'] * 1.0 / df['pop10'].sum() * 100

	cols = ['pwhite00', 'pblack00', 'pasian00', 'phisp00', 'pother00',
			'pwhite10', 'pblack10', 'pasian10', 'phisp10', 'pother10', 
			'pop00', 'pop10', 'pop00pct', 'pop10pct', 
			'E00', 'E10', 'Hgm00', 'Hgm10', 'Hgm00pct', 'Hgm10pct', 'HgmChg']

	temp = temp[cols].round(3)

	if count==0:
		final_df = temp
	elif count >= 1:
		final_df = pd.concat([final_df, temp])
	# print final_df
	count+=1


outf = "/home/eric/Documents/franklin/fowler/individual_loss_metro_stats.xlsx"
writer = pd.ExcelWriter(outf) 
final_df.to_excel(writer, sheet_name="data")


final_df = final_df.reset_index()
final_df['HgmChgBinary'] = 'increase'
final_df.loc[final_df['HgmChg']<0, 'HgmChgBinary'] = 'decrease'

print pd.crosstab(index=[final_df['princip'], final_df['loss_flag']], columns=final_df['HgmChgBinary'], normalize='index') * 100

print pd.crosstab(index=[final_df['princip'], final_df['loss_flag']], columns=final_df['HgmChgBinary'])


con.close()