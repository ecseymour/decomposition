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
	# calc btw regions component: GROWING vs SHRINKING CBSAs as "regions"
	############################################################
	# group CBSAs into growing/shrinking using aggregated place pop
	cols = ['cbsa_geoid10','pop{}'.format(start), 'pop{}'.format(end), 'white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	cbsas = df[cols].groupby('cbsa_geoid10').sum()
	cbsas['chg{}{}'.format(start,end)] = (cbsas['pop{}'.format(end)] - cbsas['pop{}'.format(start)]) * 1.0 / cbsas['pop{}'.format(start)] * 100
	cbsas['chg_cat'] = -99
	cbsas.loc[cbsas['chg{}{}'.format(start,end)] < 0, 'chg_cat'] = 'loss'
	cbsas.loc[cbsas['chg{}{}'.format(start,end)] >= 0, 'chg_cat'] = 'growth'
	print "+" * 25
	print "count in each CBSA group"
	print cbsas.groupby('chg_cat').size()
	# aggregate CBSAs based on growth/loss
	grouped_cbsas = cbsas.groupby('chg_cat').sum()
	cols = ['white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	for c in cols:
		grouped_cbsas['p{}'.format(c)] = grouped_cbsas['{}'.format(c)] * 1.0 / grouped_cbsas[cols].sum(axis=1)
	# calc diversity for aggregated growth/loss CBSAs
	grouped_cbsas['Er'] = 0
	for c in cols:
		grouped_cbsas.loc[grouped_cbsas['p{}'.format(c)] > 0.0, 'Er'] += grouped_cbsas['p{}'.format(c)] * np.log(1.0/grouped_cbsas['p{}'.format(c)])
	grouped_cbsas['Er'] = grouped_cbsas['Er'] / np.log(len(cols))
	# decomposition
	# calc Hru: compare each region to us
	Hru = 0
	for i, x in grouped_cbsas.iterrows():
		Hru += x['pop{}'.format(end)] * (Eu - x['Er'])
	Hru = (1.0 / (Tu * Eu)) * Hru 
	print "Hru r = growth/loss CBSAs: {}".format(round(Hru,3))
	print "+" * 25
	############################################################
	# calc btw CBSA component
	############################################################
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
	############################################################
	# calc btw CBSA component comparing CBSAs to higher level CBSA growth/loss grouping
	############################################################
	# for each CBSA growth/loss group, compare Em to Er, summing across regions
	# need to compare CBSAs to region in which they are nested
	Hmr = 0
	for g in ['growth', 'loss']:
		print g, grouped_cbsas.loc[g]['Er']
		for i, x in cbsas.loc[cbsas['chg_cat']==g].iterrows():
			Hmr +=  x['pop{}'.format(end)] * ( grouped_cbsas.loc[g]['Er'] - x['Em'] )
	Hmr = (1.0 / (Tu * Eu)) * Hmr
	print "Hmr r = growth/loss CBSAs: {}".format(round(Hmr,3))
	############################################################
	# calc btw group component within CBSAs
	############################################################
	# create groups based on pop change past decade
	df['chg{}{}'.format(start,end)] = (df['pop{}'.format(end)] - df['pop{}'.format(start)]) * 1.0 / df['pop{}'.format(start)] * 100
	df['chg_cat'] = 0
	df.loc[df['chg{}{}'.format(start,end)] < 0, 'chg_cat'] = 'loss'
	df.loc[df['chg{}{}'.format(start,end)] >= 0, 'chg_cat'] = 'growth'
	print "+" * 5
	print "count in each group"
	print df.groupby('chg_cat').size()

	# add level for growth/loss CBSAs
	# calc btw places w/in growth CBSAs AND btw places w/in loss CBSAs
	# enter as separate rows in table

	data_dict = {}
	levels = ['growth', 'loss']
	for g in levels:
		data_dict[g] = {'loss' : 0, 'growth': 0, 'total': 0}
		# separately iterate through growth and loss CBSAs
		for i, x in cbsas.loc[cbsas['chg_cat']==g].iterrows():
			# for each cbsa (in each CBSA growth/loss category), compare group diversity to cbsa diversity (btw groups w/in metros)
			for g2 in levels:
				cols = ['chg_cat', 'pop{}'.format(end), 'white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
				groups = df.loc[(df['cbsa_geoid10']==i) & (df['chg_cat']==g2)][cols].groupby('chg_cat').sum()
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
					data_dict[g][g2] += groups['pop{}'.format(end)].item() * (x['Em'] - groups['Eg'].item())
					data_dict[g]['total'] += groups['pop{}'.format(end)].item() * (x['Em'] - groups['Eg'].item())				

	# scale H
	for g in levels:
		for g2 in ['growth', 'loss', 'total']:
			data_dict[g][g2] = round((1.0 / Tu*Eu) * data_dict[g][g2], 5)
	
	print '+' * 2 
	for k, v in data_dict.iteritems():
		print '+' * 2 
		print k, v
	print '+' * 2 


	# calc total Hgm
	levels = ['growth', 'loss']
	Hgm = 0
	for g in levels:
		for i, x in cbsas.iterrows():
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
				Hgm += groups['pop{}'.format(end)].item() * (x['Em'] - groups['Eg'].item())

	Hgm = (1.0 / (Tu * Eu)) * Hgm
	print "Total Hgm: {}".format(Hgm)
	print '+' * 2 
	# calc total Hgm
	# print "Hgm {} {}: {}".format(g,g2 Hgm)
	############################################################
	# calc btw place component within CBSAs
	############################################################
	Hpm = 0
	for i, x in cbsas.iterrows():
		for i2, x2 in df.loc[df['cbsa_geoid10']==i].iterrows():
			Hpm += x2['pop{}'.format(end)] * (x['Em'] - x2['Ep'])

	Hpm = (1.0 / (Tu * Eu)) * Hpm
	print "Hpm: {}".format(round(Hpm,3))

con.close()