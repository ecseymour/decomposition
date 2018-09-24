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
start = years[0]
end = years[1]
qry = "SELECT * FROM subset_places_{}_{};".format(start, end)
df = pd.read_sql(qry, con, index_col='nhgisplace10')
con.close()
print "places in subset: {}".format(len(df))

from collections import OrderedDict
cbsa_dict = OrderedDict()
for k, v in df[['cbsa_geoid10', 'name10']].sort_values('name10').iterrows():
    if v['cbsa_geoid10'] not in cbsa_dict:
        cbsa_dict[v['cbsa_geoid10']] = v['name10']

# init plot
fig, axes = plt.subplots(nrows=15, ncols=5, sharey=True, figsize=(12,22), squeeze=False)
fig.tight_layout()
# plt.subplots_adjust(hspace = 1)
axli = axes.flatten()
plt.rc('xtick', labelsize=8)
############################################################
# calc metro-level segregation
############################################################
count=0
for k, v in cbsa_dict.iteritems():
	print k, v
	if count>=71:
		pass
	else:
		temp = df.loc[df['cbsa_geoid10']==k]
		# sum race/ethnicity categories across places
		cols = ['white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
		total_cbsa = pd.DataFrame(temp[cols].sum(axis=0))
		total_cbsa.columns = ['count']
		# calc share of total in each category
		total_cbsa['share'] = total_cbsa['count'] * 1.0 / total_cbsa['count'].sum(axis=0)
		# calc entropy
		Em = 0
		for i, x in total_cbsa.iterrows():
			Em += x['share'] * np.log(1.0/x['share'])
		# scale measure
		Em = Em / np.log(len(cols))
		# print "Metro scaled entropy: {}".format(round(Em,3))
		############################################################
		# calc Hpm: compare each place to metro
		############################################################
		Tm = temp['pop{}'.format(end)].sum(axis=0)
		Hpm = 0
		for i, x in temp.iterrows():
			Hpm += x['pop{}'.format(end)] * (Em - x['Ep'])
		Hpm = (1.0 / (Tm * Em)) * Hpm
		# print "Hpm: {}".format(round(Hpm,3))
		############################################################
		# for each place calc diff from metro E
		############################################################
		temp['diff'] = 0
		for i, x in temp.iterrows():
			temp.at[i, 'diff'] = x['pop{}'.format(end)] * (Em - x['Ep'])
			# print x['pop{}'.format(end)] * (Em - x['Ep'])
		temp['diff'] = temp['diff'] * (1.0 / (Hpm * Tm))
		# print temp[['diff']].head()
		############################################################
		# create groups based on pop change past decade
		############################################################
		temp['chg{}{}'.format(start,end)] = (temp['pop{}'.format(end)] - temp['pop{}'.format(start)]) * 1.0 / temp['pop{}'.format(start)] * 100
		temp['chg_cat'] = 0
		temp.loc[temp['chg{}{}'.format(start,end)] < 0, 'chg_cat'] = 'loss'
		temp.loc[temp['chg{}{}'.format(start,end)] >= 0, 'chg_cat'] = 'growth'
		############################################################
		# group metros into growth loss
		############################################################
		cbsa_chg = (temp['pop{}'.format(end)].sum() - temp['pop{}'.format(start)].sum()) * 1.0 / temp['pop{}'.format(start)].sum() * 100
		############################################################
		# plot data
		############################################################
		ax=sns.boxplot(x=temp['diff'], y=temp['chg_cat'], order=['growth', 'loss'], ax=axli[count])
		cbsa_name = v
		if "-" in v:
			cbsa_name = cbsa_name.split("-")[0]
		else:
			cbsa_name = cbsa_name.split(",")[0]
		if cbsa_chg >= 0:
			ax.set_title('{} (H = {})'.format(cbsa_name, round(Hpm,3)), color='green')
		else:
			ax.set_title('{} (H = {})'.format(cbsa_name, round(Hpm,3)), color='red')		
		# ax.title.set_text('{} (H = {})'.format(cbsa_name, round(Hpm,3)))			
		ax.set_xlabel('')
		ax.title.set_size(8)
	count+=1


outFile = "/home/eric/Documents/franklin/fowler/figures/metro_boxplots.png"
plt.savefig(outFile, dpi=600, bbox_inches='tight')