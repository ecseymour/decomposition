'''
calc entropy at every scale
* US
* region
* CBSA
* place
'''
from pysqlite2 import dbapi2 as sql
import pandas as pd
import numpy as np

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

# iterate over all periods and collect output calculations
periods = [ ['80', '90'], ['90', '00'], ['00', '10'] ]

for p in periods:
	print "+" * 50
	print p
	start = p[0]
	end = p[1]
	# collect places data
	qry = "SELECT * FROM subset_places_{}_{};".format(start, end)
	df = pd.read_sql(qry, con, index_col='nhgisplace{}'.format(end))
	print "places in subset: {}".format(len(df))
	df['US'] = 'US'
	############################################################
	# calc H for US
	############################################################
	# sum race/ethnicity categories across places
	cols = ['white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	total_us = pd.DataFrame(df[cols].sum(axis=0))
	total_us.columns = ['count']
	# calc share of total in each category
	total_us['share'] = total_us['count'] * 1.0 / total_us['count'].sum(axis=0)
	# calc entropy
	Eu = 0
	for i, x in total_us.iterrows():
		Eu += x['share'] * np.log(1.0/x['share'])
	# scale measure
	Eu = Eu / np.log(len(cols))
	print "US scaled entropy: {}".format(round(Eu,3))
	############################################################
	# calc Hpu: compare each place to US
	############################################################
	Tu = df['pop{}'.format(end)].sum(axis=0)
	Hpu = 0
	for i, x in df.iterrows():
		Hpu += x['pop{}'.format(end)] * (Eu - x['Ep'])
	Hpu = (1.0 / (Tu * Eu)) * Hpu
	print "Hpu: {}".format(round(Hpu,3))
	############################################################
	# calc btw region component
	############################################################
	cols = ['region', 'pop{}'.format(end), 'white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	# sum race/ethnicity categories across regions
	regions = df[cols].groupby('region').sum()
	# calc E for each region
	# create region level percentages
	cols = ['white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	for c in cols:
		regions['p{}'.format(c)] = regions['{}'.format(c)] * 1.0 / regions[cols].sum(axis=1)

	regions['Er'] = 0
	for c in cols:
		regions.loc[regions['p{}'.format(c)] > 0.0, 'Er'] += regions['p{}'.format(c)] * np.log(1.0/regions['p{}'.format(c)])
	regions['Er'] = regions['Er'] / np.log(len(cols))

	# calc Hru: compare each region to us
	Hru = 0
	for i, x in regions.iterrows():
		Hru += x['pop{}'.format(end)] * (Eu - x['Er'])
	Hru = (1.0 / (Tu * Eu)) * Hru 
	print "Hru: {}".format(round(Hru,3))
	############################################################
	# calc btw CBSA component
	############################################################
	cols = ['cbsa_geoid10', 'pop{}'.format(end), 'white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	cbsas = df[cols].groupby('cbsa_geoid10').sum()
	# calc E for each CBSA
	# create CBSA level percentages
	cols = ['white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	for c in cols:
		cbsas['p{}'.format(c)] = cbsas['{}'.format(c)] * 1.0 / cbsas[cols].sum(axis=1)
	# calc E
	cbsas['Em'] = 0
	for c in cols:
		cbsas.loc[cbsas['p{}'.format(c)] > 0.0, 'Em'] += cbsas['p{}'.format(c)] * np.log(1.0/cbsas['p{}'.format(c)])
	cbsas['Em'] = cbsas['Em'] / np.log(len(cols))
	# merge CBSAs with regions
	cbsa_regions = df[['cbsa_geoid10', 'region']].groupby('cbsa_geoid10').min()
	cbsas = pd.merge(cbsas, cbsa_regions, left_index=True, right_index=True)
	# for each region, compare Em to Er, summing across regions
	# need to compare CBSAs to region in which they are nested
	Hmr = 0
	for i, x in regions.iterrows():
		Tr = x['pop{}'.format(end)] 
		Er = x['Er']
		for i2, x2 in cbsas.loc[cbsas['region']==i].iterrows(): 
			Tm = x2['pop{}'.format(end)]
			Em = x2['Em']
			Hmr += Tm * (Er - Em)
	Hmr = (1.0 / (Tu * Eu)) * Hmr 

	print "Hmr: {}".format(round(Hmr,3))
	# residual is within CBSA component
	print "w/in Ms: {}".format(round(Hpu - Hru - Hmr, 3))
	############################################################
	# calc btw group component within CBSAs
	############################################################
	# create dict to save for analysis for every cbsa
	group_dict = {}

	# create groups based on pop change past decade
	df['chg{}{}'.format(start,end)] = (df['pop{}'.format(end)] - df['pop{}'.format(start)]) * 1.0 / df['pop{}'.format(start)] * 100
	df['chg_cat'] = 0
	df.loc[df['chg{}{}'.format(start,end)] < 0, 'chg_cat'] = 'loss'
	df.loc[ df['chg{}{}'.format(start,end)] >= 0, 'chg_cat'] = 'growth'
	print "+" * 5
	print "count in each group"
	print df.groupby('chg_cat').size()

	print cbsas.head()

	groups = df['chg_cat'].unique()
	for g in groups:
		print "+" * 5
		print g

		Hgm = 0
		for i, x in cbsas.iterrows():
			if i not in group_dict:
				group_dict[i] = {}
				group_dict[i]['name10'] = None
			# agg places by group for each cbsa
			cols = ['chg_cat', 'pop{}'.format(end), 'white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
			groups = df.loc[(df['cbsa_geoid10']==i) & (df['chg_cat']==g)][cols].groupby('chg_cat').sum()
			if groups.empty:
				pass
			else:
				cols = ['white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
				for c in cols:
					groups['p{}'.format(c)] = groups['{}'.format(c)] * 1.0 / groups[cols].sum(axis=1)
				groups['Eg'] = 0
				for c in cols:
					groups.loc[groups['p{}'.format(c)] > 0.0, 'Eg'] += groups['p{}'.format(c)] * np.log(1.0/groups['p{}'.format(c)])
				groups['Eg'] = groups['Eg'] / np.log(len(cols))
				Hgm += groups['pop{}'.format(end)] * (x['Em'] - groups['Eg'])

				group_dict[i]['cbsa'] = x['Em']
				group_dict[i]["{}_E".format(g)] = groups['Eg'].item()
				group_dict[i]["{}_Pop".format(g)] = groups['pop{}'.format(end)].item()

		for i, x in df.iterrows():
			group_dict[x['cbsa_geoid10']]['name10'] = x['name10']
					
		group_data = pd.DataFrame.from_dict(group_dict, orient='index')
		data_dir = "/home/eric/Documents/franklin/fowler/data/"
		group_data.to_csv(data_dir+'cbsa_group_data_{}-{}.csv'.format(start, end), index_label='FIPS')

		Hgm = (1.0 / (Tu * Eu)) * Hgm
		print "Hgm: {}".format(Hgm)

	############################################################
	# calc btw place component within CBSAs
	############################################################
	Hpm = 0
	for i, x in cbsas.iterrows():
		cols = ['chg_cat', 'pop{}'.format(end), 'white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
		for i2, x2 in df.loc[df['cbsa_geoid10']==i].iterrows():
			Hpm += x2['pop{}'.format(end)] * (x['Em'] - x2['Ep'])

	Hpm = (1.0 / (Tu * Eu)) * Hpm
	print "Hpm: {}".format(Hpm)

con.close()