'''
segregation decomp using ganning's list of shrinking cities
decompositions:
1) btw regions
2) btw metros /w and w/out shrinking city
then restrict to metros w/ 1+ shrinking city,
decomp seg btw shrinking cities and other places

ES question: what about places losing pop. that do no meet
50k pop. threshold for being considered a shrinking city?
should we not group them with the large shrinking city of cities in a metro?
grouping smaller shrinking cities/places with growing places will make that group
more heterogeneous. Do we have a substantive reason to be concerned with 
large shrinking cities alone?
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
########################################################################
# create list of cbsas containing one of ganning's shrinking cities
cur.execute("SELECT cbsa_geoid10 FROM shrinking_cities;")
results = cur.fetchall()
shrinking_cbsa_lst = []
for row in results:
	shrinking_cbsa_lst.append(row[0])
print shrinking_cbsa_lst
print len(shrinking_cbsa_lst)

# create list of of ganning's shrinking cities
cur.execute("SELECT NHGISCODE FROM shrinking_cities;")
results = cur.fetchall()
shrinking_city_lst = []
for row in results:
	shrinking_city_lst.append(row[0])
print shrinking_city_lst
print len(shrinking_city_lst)
########################################################################
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

	# calc Hru: compare each region to US
	Hru = 0
	for i, x in regions.iterrows():
		Hru += x['pop{}'.format(end)] * (Eu - x['Er'])
	Hru = (1.0 / (Tu * Eu)) * Hru 
	print "Hru: {}".format(round(Hru,3))
	############################################################
	# calc btw loss metro component
	# REVISE TO CLASSIFY LOSS METROS AS THOSE CONTAINING ONE OR MORE OF GANNING'S SHRINKING CITIES
	############################################################
	# create msa:name dict
	msa_dict = {}
	for i, x in df.iterrows():
		if x['cbsa_geoid10'] not in msa_dict:
			msa_dict[x['cbsa_geoid10']]=x['name10']

	# group CBSAs into growing/shrinking BASED ON PRESENCE OF GANNING SHRINKING CITY
	# need to relate metros to ganning's list, selecting those that contain a shrinking city as a shrinking metro	
	cols = ['cbsa_geoid10','pop{}'.format(start), 'pop{}'.format(end), 'white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	cbsas = df[cols].groupby('cbsa_geoid10').sum()

	# check if cbsas - now an index val after the groupby - are in list of cbsas w/ a shrinking city, created at top of script
	cbsas.loc[cbsas.index.isin(shrinking_cbsa_lst), 'chg_cat'] = 'loss'
	cbsas.loc[~cbsas.index.isin(shrinking_cbsa_lst), 'chg_cat'] = 'growth'

	print "+" * 5
	print "count loss/growth CBSAs"
	print cbsas.groupby('chg_cat').size()
	print "+" * 5

	# OLD CODE FOR CLASSIFYING BY CBSA AGGREGATE PLACE POP CHANGE
	# cbsas['chg{}{}'.format(start,end)] = (cbsas['pop{}'.format(end)] - cbsas['pop{}'.format(start)]) * 1.0 / cbsas['pop{}'.format(start)] * 100
	# cbsas['chg_cat'] = -99
	# cbsas.loc[cbsas['chg{}{}'.format(start,end)] < 0, 'chg_cat'] = 'loss'
	# cbsas.loc[cbsas['chg{}{}'.format(start,end)] >= 0, 'chg_cat'] = 'growth'
	
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
	# REVISE TO COMPARE SHRINKING CITIES ALONE TO OTHER PLACES
	############################################################
	# create groups based on pop change past decade
	# what about other shrinking places? what about places w/out shrinking cities? I guess we are excluding them for now?
	df['chg{}{}'.format(start,end)] = (df['pop{}'.format(end)] - df['pop{}'.format(start)]) * 1.0 / df['pop{}'.format(start)] * 100
	df['chg_cat'] = 0

	df.loc[df.index.isin(shrinking_city_lst), 'chg_cat'] = 'loss'
	df.loc[~df.index.isin(shrinking_city_lst), 'chg_cat'] = 'growth'

	# OLD CODE
	# df.loc[df['chg{}{}'.format(start,end)] < 0, 'chg_cat'] = 'loss'
	# df.loc[df['chg{}{}'.format(start,end)] >= 0, 'chg_cat'] = 'growth'
	
	print "+" * 5
	print "count in each group"
	print df.groupby('chg_cat').size()
	print "+" * 5

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
				# if g=='loss' and g2=='loss' and not groups.empty:
				# 	# if groups['Eg'].item() > x['Em']:	
				# 	try:
				# 		print i, msa_dict[i], g2, round(groups['Eg'].item(), 3), round(x['Em'],3)
				# 	except:
				# 		print i,  msa_dict[i], g2

	# scale H
	for g in levels:
		for g2 in ['growth', 'loss', 'total']:
			data_dict[g][g2] = round((1.0 / Tu*Eu) * data_dict[g][g2], 5)
	
	print '+' * 2 
	for k, v in data_dict.iteritems():
		print k, v
	print '+' * 2

con.close()