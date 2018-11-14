'''
From Chris

1. Distribution of H for places in loss city metros with loss city flagged
--note these could be population weighted or not. 
I tend to prefer not, but weighted could be a robustness check. 
Since we are pulling out loss cities anyway 
some of the problems should be reduced.
	1. 1 decade at a time
	2. Showing change in loss city as a line with 
	overlapping diversity profiles for places in same metro
'''

from pysqlite2 import dbapi2 as sql
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
from collections import OrderedDict
import seaborn as sns

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

'''
pull data for loss metros only
join on cbsa code, not place code, to retain all places in loss cbsas
but also need to flag shrinking city: add as separate data element?
'''

# loop through decades

decades = ['80_90', '90_00', '00_10']

for d in decades:

	print d

	qry = '''
	SELECT DISTINCT A.*
	FROM subset_places_{} AS A JOIN shrinking_cities AS B
	ON A.cbsa_geoid10 = B.cbsa_geoid10
	'''.format(d)
	df = pd.read_sql(qry, con)

	print df.groupby('name10').size()
	print len(df.groupby('name10').size())
	print df.groupby('name10').size().sum()

	# create dict of cbsas to iterate through in plot
	cbsa_dict = OrderedDict()
	for k, v in df[['cbsa_geoid10', 'name10']].sort_values('name10').iterrows():
		if v['cbsa_geoid10'] not in cbsa_dict:
			cbsa_dict[v['cbsa_geoid10']] = v['name10']

	# 26 shrinking metros: 4 x 7 plot?
	fig, axes = plt.subplots(nrows=7, ncols=4, sharex=True, sharey=True, figsize=(12,16), squeeze=False)
	axli = axes.flatten()

	count = 0
	for k, v in cbsa_dict.iteritems():
		if count>=26:
			pass
		else:
			temp=df.loc[df['cbsa_geoid10']==k]
			ax=sns.kdeplot(data=temp['Ep'], label='all', ax=axli[count], legend=False)
			

			# get E for each shrinking city in CBSA
			qry = '''
			SELECT A.nhgisplace{} as nhgisplace, Ep
			FROM subset_places_{} AS A JOIN shrinking_cities AS B
			ON A.nhgisplace{} = B.NHGISCODE
			WHERE A.cbsa_geoid10 = '{}'
			'''.format(d[-2:], d, d[-2:], k)
			temp2 = pd.read_sql(qry, con, index_col='nhgisplace')
			count2 = 0
			for i, x in temp2.iterrows():
				if count2==0:
					ax.axvline(x['Ep'], color='red', alpha=0.5, label='shrinking')
				else:
					ax.axvline(x['Ep'], color='red', alpha=0.5)
				count2+=1

			cbsa_name = v.split(',')[0]
			if '-' in cbsa_name:
				cbsa_name = cbsa_name.split('-')[0] + ', ' + v.split(',')[1]
			else:
				cbsa_name = cbsa_name + ', ' + v.split(',')[1]
			ax.title.set_text(cbsa_name)
			ax.title.set_size(10)
			
			ax.set_ylim(0,10)
			ax.set_xlim(-0.25,1.0)
		
		count+=1

	# remove unused subplots
	for i in range(26, 28):
		fig.delaxes(axes.flatten()[i])

	handles, labels = ax.get_legend_handles_labels()
	fig.legend(handles, labels, loc='lower right', borderaxespad=0., fontsize=18, bbox_to_anchor=(0.9, 0.1),
			   bbox_transform=plt.gcf().transFigure, ncol=3)
	
	year = '1990'
	if d=='00_10':
		year = '2010'
	elif d=='90_00':
		year = '2000'

	plt.suptitle('Diversity Profiles {}'.format(year), size=18)
	fig.tight_layout(rect=[0, 0.03, 1, 0.95])

	outFile = "/home/eric/Documents/franklin/fowler/figures/diversity_profiles_shrinking_cities_{}.jpg".format(d)
	plt.savefig(outFile, bbox_inches='tight')


con.close()