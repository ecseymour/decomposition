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

qry = "SELECT * FROM subset_places;"
df = pd.read_sql(qry, con, index_col='nhgisplace10')
############################################################
############################################################
# calc H for US by place
############################################################
############################################################
'''
calc E for US
sum across places and calc E
'''
cols = ['white10', 'black10', 'asian10', 'hisp10', 'other10']
total_us = pd.DataFrame(df[cols].sum(axis=0))
total_us.columns = ['count']
total_us['share'] = total_us['count'] * 1.0 / total_us['count'].sum(axis=0)
# print total_us
Eu = 0
for i, x in total_us.iterrows():
	Eu+=x['share'] * np.log(1.0/x['share'])

Eu = Eu / np.log(len(cols))
print "US scaled entropy: {}".format(round(Eu,3))
############################################################
# calc E for places and compare to US
'''
calc scaled E for each place, add as df col
'''
# create place level percentages
for c in cols:
	df['p{}'.format(c)] = df['{}'.format(c)] * 1.0 / df[cols].sum(axis=1)

df['Ep'] = 0
for c in cols:
	df.loc[df['p{}'.format(c)] > 0.0, 'Ep'] += df['p{}'.format(c)] * np.log(1.0/df['p{}'.format(c)])
df['Ep'] = df['Ep'] / np.log(len(cols))

# print df[['place10', 'Ep']].sort_values('Ep', ascending=False).head(10)

# calc Hpu: compare each place to us
Tu = df['pop10'].sum(axis=0)
Hpu = 0
for i, x in df.iterrows():
	Hpu += x['pop10'] * (Eu - x['Ep'])
Hpu = (1.0 / (Tu * Eu)) * Hpu 

print "Hpu: {}".format(round(Hpu,3))
############################################################
############################################################
# calc btw region component
############################################################
############################################################
'''
calc btw regions H
aggregate by region, compare to us
'''
cols = ['region', 'pop10', 'white10', 'black10', 'asian10', 'hisp10', 'other10']
regions = df[cols].groupby('region').sum()

# calc E for each region
# create region level percentages
cols = ['white10', 'black10', 'asian10', 'hisp10', 'other10']
for c in cols:
	regions['p{}'.format(c)] = regions['{}'.format(c)] * 1.0 / regions[cols].sum(axis=1)

regions['Er'] = 0
for c in cols:
	regions.loc[regions['p{}'.format(c)] > 0.0, 'Er'] += regions['p{}'.format(c)] * np.log(1.0/regions['p{}'.format(c)])
regions['Er'] = regions['Er'] / np.log(len(cols))

# calc Hru: compare each region to us
Tu = df['pop10'].sum(axis=0)
Hru = 0
for i, x in regions.iterrows():
	Hru += x['pop10'] * (Eu - x['Er'])
Hru = (1.0 / (Tu * Eu)) * Hru 

print "Hru: {}".format(round(Hru,3))
############################################################
############################################################
# calc btw CBSA component
############################################################
############################################################
'''
agg by cbsa, compare to region. sum across regions
'''
cols = ['cbsa_geoid10', 'pop10', 'white10', 'black10', 'asian10', 'hisp10', 'other10']
cbsas = df[cols].groupby('cbsa_geoid10').sum()

# calc E for each CBSA
# create CBSA level percentages
cols = ['white10', 'black10', 'asian10', 'hisp10', 'other10']
for c in cols:
	cbsas['p{}'.format(c)] = cbsas['{}'.format(c)] * 1.0 / cbsas[cols].sum(axis=1)

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
	Tr = x['pop10'] 
	Er = x['Er']
	for i2, x2 in cbsas.loc[cbsas['region']==i].iterrows(): 
		Tm = x2['pop10']
		Em = x2['Em']
		Hmr += Tm * (Er - Em)
Hmr = (1.0 / (Tu * Eu)) * Hmr 

print "Hmr: {}".format(round(Hmr,3))

# residual is within CBSA component
print "within metros: {}".format(round(Hpu - Hru - Hmr, 3))
############################################################
############################################################
# calc btw group component within CBSAs
############################################################
############################################################
'''
compare group entropy to cbsa entropy
need to group by cbsa, then by group, 
then compare each group to the cbsa in which it is nested
can iterate through each cbsa, aggregating places by group,
then compare to cbsa values
DOES EACH GROUP LEVEL GET ENTERED SEPARATELY?
'''
# create groups based on pop change past decade
df['chg0010'] = (df['pop10'] - df['pop00']) * 1.0 / df['pop00'] * 100
df['chg_cat'] = 0
df.loc[df['chg0010'] < 0, 'chg_cat'] = 'loss'
# df.loc[ (df['chg0010'] >= 0) & (df['chg0010'] < 5), 'chg_cat'] = 'growth1'
df.loc[ df['chg0010'] >= 0, 'chg_cat'] = 'growth1'

# # create groups based on pop change past decade
# df['chg_cat'] = 0
# df.loc[df['pop10'] >= 50000, 'chg_cat'] = '50k+'
# df.loc[df['pop10'] < 50000, 'chg_cat'] = 'under50k'

# print df['chg0010'].describe()

# print "+" * 50
# # how many in each category
# print df.groupby('chg_cat').size()
# print "+" * 50

# for each cbsa, agg by group

groups = df['chg_cat'].unique()
for g in groups:
	print "+" * 50
	print g

	Hgm = 0
	for i, x in cbsas.iterrows():
		# agg places by group for each cbsa
		cols = ['chg_cat', 'pop10', 'white10', 'black10', 'asian10', 'hisp10', 'other10']
		groups = df.loc[(df['cbsa_geoid10']==i) & (df['chg_cat']==g)][cols].groupby('chg_cat').sum()

		# calc E for each group level
		# gen percentages BEWARE OF ZEROS
		cols = ['white10', 'black10', 'asian10', 'hisp10', 'other10']
		for c in cols:
			groups['p{}'.format(c)] = groups['{}'.format(c)] * 1.0 / groups[cols].sum(axis=1)

		groups['Eg'] = 0
		for c in cols:
			groups.loc[groups['p{}'.format(c)] > 0.0, 'Eg'] += groups['p{}'.format(c)] * np.log(1.0/groups['p{}'.format(c)])
		groups['Eg'] = groups['Eg'] / np.log(len(cols))
		# print groups[['pop10', 'Em']]		
		Hgm += groups['pop10'] * (Em - groups['Eg'])

	Hgm = (1.0 / (Tu * Eu)) * Hgm
	print "Hgm: {}".format(Hgm)





con.close()