from pysqlite2 import dbapi2 as sql
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt
import seaborn as sns

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()


years = ['00', '10']
# years = ['80', '90']

start = years[0]
end = years[1]
qry = "SELECT * FROM subset_places_{}_{};".format(start, end)
df = pd.read_sql(qry, con, index_col='nhgisplace{}'.format(end))
con.close()
print "places in subset: {}".format(len(df))

from collections import OrderedDict
cbsa_dict = OrderedDict()
for k, v in df[['region', 'region_name']].sort_values('region').iterrows():
    if v['region'] not in cbsa_dict:
        cbsa_dict[v['region']] = v['region_name']

# init plot
fig, axes = plt.subplots(nrows=2, ncols=2, sharey=True, figsize=(12,12))
# fig.tight_layout()
# plt.subplots_adjust(hspace = 1)
axli = axes.flatten()
############################################################
# calc metro-level segregation
############################################################
count=0
for k, v in cbsa_dict.iteritems():
	print k, v
	temp = df.loc[df['region']==k]
	# sum race/ethnicity categories across places
	cols = ['white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	total_region = pd.DataFrame(temp[cols].sum(axis=0))
	total_region.columns = ['count']
	# calc share of total in each category
	total_region['share'] = total_region['count'] * 1.0 / total_region['count'].sum(axis=0)
	# calc entropy
	Er = 0
	for i, x in total_region.iterrows():
		Er += x['share'] * np.log(1.0/x['share'])
	# scale measure
	Er = Er / np.log(len(cols))
	# print Em
	############################################################
	# calc Hmr: compare each metro to region: need to agg and code metros
	# group CBSAs into growing/shrinking using aggregated place pop
	cols = ['cbsa_geoid10','pop{}'.format(start), 'pop{}'.format(end), 'white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
	cbsas = temp[cols].groupby('cbsa_geoid10').sum()
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
	###########################################################
	Tr = temp['pop{}'.format(end)].sum(axis=0)
	Hmr = 0
	for i, x in cbsas.iterrows():
		Hmr += x['pop{}'.format(end)] * (Er - x['Em'])
	Hmr = (1.0 / (Tr * Er)) * Hmr
	print "Hmr: {}".format(round(Hmr,3))
	############################################################
	# for each place calc diff from metro E
	############################################################
	cbsas['diff'] = 0
	for i, x in cbsas.iterrows():
		cbsas.at[i, 'diff'] = x['pop{}'.format(end)] * (Er - x['Em'])
		# print x['pop{}'.format(end)] * (Em - x['Ep'])
	cbsas['diff'] = cbsas['diff'] * (1.0 / (Hmr * Tr))
	print cbsas.loc[cbsas['chg_cat']=='growth']['diff']
	# ############################################################
	# # create groups based on pop change past decade
	# ############################################################
	# temp['chg{}{}'.format(start,end)] = (temp['pop{}'.format(end)] - temp['pop{}'.format(start)]) * 1.0 / temp['pop{}'.format(start)] * 100
	# temp['chg_cat'] = 0
	# temp.loc[temp['chg{}{}'.format(start,end)] < 0, 'chg_cat'] = 'loss'
	# temp.loc[temp['chg{}{}'.format(start,end)] >= 0, 'chg_cat'] = 'growth'
	# ############################################################
	# # group metros into growth loss
	# ############################################################
	# cbsa_chg = (temp['pop{}'.format(end)].sum() - temp['pop{}'.format(start)].sum()) * 1.0 / temp['pop{}'.format(start)].sum() * 100
	# ############################################################
	# # plot data
	# ############################################################
	ax=sns.boxplot(x=cbsas['diff'], y=cbsas['chg_cat'], order=['growth', 'loss'], ax=axli[count])
	cbsa_name = v
	ax.set_title('{} (H = {})'.format(cbsa_name, round(Hmr,3)))		
	ax.set_xlabel('')
	ax.set_ylabel('')
	ax.tick_params(axis='both')
	# ax.title.set_size(8)
	count+=1

fig.suptitle(r"Decomposition of '{} regional segregation by growth/loss metros".format(end))
outFile = "/home/eric/Documents/franklin/fowler/figures/region_boxplots.png"
plt.savefig(outFile, dpi=300, bbox_inches='tight')