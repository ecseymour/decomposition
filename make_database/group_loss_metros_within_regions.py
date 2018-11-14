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
	# calc btw loss metro component
	# REVISE TO AGG GROWTH/LOSS METROS AND CALC SEGREGATION WITHIN EACH GROUP
	############################################################
	# create msa:name dict
	msa_dict = {}
	for i, x in df.iterrows():
		if x['cbsa_geoid10'] not in msa_dict:
			msa_dict[x['cbsa_geoid10']]=x['name10']

	# group CBSAs into growing/shrinking using aggregated place pop
	cols = ['cbsa_geoid10','pop{}'.format(start), 'pop{}'.format(end), 'white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	cbsas = df[cols].groupby('cbsa_geoid10').sum()
	cbsas['chg{}{}'.format(start,end)] = (cbsas['pop{}'.format(end)] - cbsas['pop{}'.format(start)]) * 1.0 / cbsas['pop{}'.format(start)] * 100
	cbsas['chg_cat'] = -99
	cbsas.loc[cbsas['chg{}{}'.format(start,end)] < 0, 'chg_cat'] = 'loss'
	cbsas.loc[cbsas['chg{}{}'.format(start,end)] >= 0, 'chg_cat'] = 'growth'
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
	chg_levels = ['growth', 'loss']
 	for l in chg_levels:
		Hmr = 0
		for i, x in regions.iterrows():
			Tr = x['pop{}'.format(end)] 
			Er = x['Er']
			for i2, x2 in cbsas.loc[(cbsas['region']==i)&(cbsas['chg_cat']==l)].iterrows(): 
				Tm = x2['pop{}'.format(end)]
				Em = x2['Em']
				Hmr += Tm * (Er - Em)
		Hmr = (1.0 / (Tu * Eu)) * Hmr 
		print "Hmr {} metros: {}".format(l, round(Hmr,3))

	# total btw metros
	Hmr = 0
	for i, x in regions.iterrows():
		Tr = x['pop{}'.format(end)] 
		Er = x['Er']
		for i2, x2 in cbsas.loc[(cbsas['region']==i)].iterrows(): 
			Tm = x2['pop{}'.format(end)]
			Em = x2['Em']
			Hmr += Tm * (Er - Em)
	Hmr = (1.0 / (Tu * Eu)) * Hmr 
	print "Hmr all metros: {}".format(round(Hmr,3))
	############################################################
	'''
	calc btw places H separately in growing and shrinking metros
	does placing this below between metro exhaust decomposition?
	it should. sum to test
	'''
	############################################################
	# for level in growth/loss: report separate Hpm values for cbsas based on level
	for l in chg_levels:
		# for each cbsa by level
		Hpm = 0
		for i, x in cbsas.loc[cbsas['chg_cat']==l].iterrows():
			# for each place in each cbsa in each level
			for i2, x2 in df.loc[df['cbsa_geoid10']==i].iterrows():
				Hpm += x2['pop{}'.format(end)] * (x['Em'] - x2['Ep'])
		Hpm = (1.0 / (Tu * Eu)) * Hpm
		print "Hpm {} metros: {}".format(l, round(Hpm, 3))
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

				# # print vals for growing metros where loss group diversity > metro diversity	
				# if g=='growth' and g2=='loss' and not groups.empty:
				# 	if groups['Eg'].item() > x['Em']:
				# 		try:
				# 			print i, msa_dict[i], g2, round(groups['Eg'].item(), 3), round(x['Em'],3)
				# 		except:
				# 			print i,  msa_dict[i], g2

				# print vals for growing metros where loss group diversity > metro diversity	
				if g=='loss' and g2=='loss' and not groups.empty:
					# if groups['Eg'].item() > x['Em']:
					try:
						print i, msa_dict[i], g2, round(groups['Eg'].item(), 3), round(x['Em'],3)
					except:
						print i,  msa_dict[i], g2

	# scale H
	for g in levels:
		for g2 in ['growth', 'loss', 'total']:
			data_dict[g][g2] = round((1.0 / Tu*Eu) * data_dict[g][g2], 5)
	
	print '+' * 2 
	for k, v in data_dict.iteritems():
		print k, v
	print '+' * 2 

	############################################################
	# calc H separately for growth and loss metros, scaled to totals across grwoth/loss metros
	############################################################
	# group CBSAs into growing/shrinking using aggregated place pop
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
	
	print "+" * 10
	levels = ['growth', 'loss']
	# compare all places located within growth and loss metros
	for l in levels:
		Hmr = 0
		# for all growth/loss metros
		for i, x in cbsas.loc[cbsas['chg_cat']==l].iterrows():
			# for all places within growth/loss metros
			for i2, x2 in df.loc[df['cbsa_geoid10']==i].iterrows():
				# take weighted diff of metro diversity and place diversity
				Hmr += x2['pop{}'.format(end)] * (x['Em'] - x2['Ep'])
		# scale by total pop and diversity of all places inside growth/loss metros
		Hmr = Hmr * (1.0 / ( grouped_cbsas.ix[l]['Er'] * grouped_cbsas.ix[l]['pop{}'.format(end)]))
		print "scaled entropy {} metros: {}".format(l, round(grouped_cbsas.ix[l]['Er'],3))
		print "Hmr {} metros (scaled to metro group): {}".format(l, round(Hmr,3))


con.close()