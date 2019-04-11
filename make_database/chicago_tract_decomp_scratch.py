'''
test the degree to which tracts grouped 
into growth and loss categories accounts
for total between-tract segregation.
test w/ Chicago MSA for 2010 - 
identify tract loss based on 00-10 change.
pure loss - or below a threshold?

adapt script for calculating county E for tracts?

multiple loss categories? high and low loss?

separately look at black-white segregation
'''

from pysqlite2 import dbapi2 as sql
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

# import statsmodels.api as sm
# import statsmodels.formula.api as smf

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

# create list of cbsas containing one of ganning's shrinking cities
cur.execute("SELECT cbsa_geoid10, cbsa_name10 FROM shrinking_cities;")
results = cur.fetchall()
shrinking_cbsa_dict = {}
for row in results:
	shrinking_cbsa_dict[row[0]] = row[1]
# print shrinking_cbsa_dict
print len(shrinking_cbsa_dict)

output_dict = {}

# iterate through cbsas, collecting counties contained in each
for cbsa_geoid, cbsa_name in shrinking_cbsa_dict.iteritems():
	# print "+" * 60
	# print "+" * 60
	# print "+" * 60
	# print cbsa_geoid, cbsa_name

	output_dict[cbsa_geoid] = {}
	output_dict[cbsa_geoid] = {'name': cbsa_name}	
	
	county_list = []
	qry = '''
	SELECT fips5digit
	FROM cbsa_county_xwalk_15
	WHERE CBSA_Code = ?
	;
	'''
	cur.execute(qry, ([cbsa_geoid]))
	results = cur.fetchall()
	for row in results:
		county_list.append(row[0])
	# print county_list

	if len(county_list)>=2:
		county_list = tuple(county_list)
	if len(county_list)==1:
		county_list = '("'+county_list[0]+'")'

	try:

		# get tracts for counties in cbsa
		qry = '''
		SELECT GISJOIN, CL8AA2000 AS pop00, CL8AA2010 AS pop10,
		CW7AA2010 AS white,
		CW7AB2010 AS black,
		CW7AD2010 AS asian,
		CW7AG2010 + CW7AH2010 + CW7AI2010 + CW7AJ2010 + CW7AK2010 + CW7AL2010 AS hisp,
		CW7AC2010 + CW7AE2010 + CW7AF2010 AS other
		FROM nhgis_tract_data
		WHERE STATEA || COUNTYA IN {}
		AND CL8AA2000 >= 1;
		'''.format(county_list)
		# print qry
		df = pd.read_sql(qry, con, index_col='GISJOIN')

		df['ppctchg'] = (df['pop10'] - df['pop00']) * 1.0 / df['pop00'] * 100

		df['chg_cat'] = None
		df.loc[df['ppctchg'] < -15, 'chg_cat'] = 'L2'
		df.loc[(df['ppctchg'] >= -15) & (df['ppctchg'] < -5), 'chg_cat'] = 'L1'
		df.loc[(df['ppctchg'] >= -5) & (df['ppctchg'] < 10), 'chg_cat'] = 'G1'
		df.loc[df['ppctchg'] >= 10, 'chg_cat'] = 'G2'
		# print df.groupby('chg_cat').size()


		# calc pct of each category
		cols = ['white', 'black', 'asian', 'hisp', 'other']
		# cols = ['white', 'black']

		for c in cols:
			df['p{}'.format(c)] = df['{}'.format(c)] * 1.0 / df[cols].sum(axis=1)

		# calc entropy
		df['Et'] = 0
		for c in cols:
			df.loc[df['p{}'.format(c)] > 0.0, 'Et'] += df['p{}'.format(c)] * np.log(1.0/df['p{}'.format(c)])
		df['Et'] = df['Et'] / np.log(len(cols))
		###########################################################################################
		# diagnostics
		# print df.head()
		# print df['ppctchg'].describe()
		# print len(df)
		###########################################################################################
		# sum race/ethnicity categories across tracts
		metro_total = pd.DataFrame(df[cols].sum(axis=0))
		metro_total.columns = ['count']
		# calc share of total in each category
		metro_total['share'] = metro_total['count'] * 1.0 / df['pop10'].sum()

		# print metro_total
		# calc entropy
		Eu = 0
		for i, x in metro_total.iterrows():
			Eu += x['share'] * np.log(1.0/x['share'])
		# scale measure
		Eu = Eu / np.log(len(cols))
		# print "metro scaled entropy: {}".format(round(Eu,3))
		###########################################################################################
		# calc Hru: compare each tract to metro
		############################################################
		Tu = df['pop10'].sum(axis=0)
		Htu = 0
		for i, x in df.iterrows():
			Htu += x['pop10'] * (Eu - x['Et'])
		Htu = (1.0 / (Tu * Eu)) * Htu
		# print "Htu: {}".format(round(Htu,3))
		###########################################################################################
		# calc btw group H: loss/growth
		###########################################################################################
		Hgu_sum = 0
		for l in df['chg_cat'].unique().tolist():
			# print "+" * 40
			# print l
			temp = pd.DataFrame(df.loc[df['chg_cat']==l][cols].sum(axis=0))
			temp.columns = ['count']
			# calc share of total in each category
			temp['share'] = temp['count'] * 1.0 / df.loc[df['chg_cat']==l]['pop10'].sum(axis=0)
			# print temp
			# calc entropy
			Eg = 0
			for i, x in temp.iterrows():
				if x['share'] > 0:
					Eg += x['share'] * np.log(1.0/x['share'])
			# scale measure
			Eg = Eg / np.log(len(cols))
			# print "metro {} group entropy: {}".format(l, round(Eg,3))
			# calc Hgu
			Tg = df.loc[df['chg_cat']==l]['pop10'].sum(axis=0)
			Hgu = Tg * (Eu - Eg)
			Hgu = (1.0 / (Tu * Eu)) * Hgu
			# print Hgu, (Hgu * 1.0 / Htu) * 100
			Hgu_sum+=Hgu

		# print "+" * 40
		# print Hgu_sum, (Hgu_sum * 1.0 / Htu) * 100
		# print "+" * 40

		output_dict[cbsa_geoid]['decomp'] = (Hgu_sum * 1.0 / Htu) * 100
		output_dict[cbsa_geoid]['tract_share'] = len(df.loc[df['ppctchg']<0]) * 1.0 / len(df) * 100 
		output_dict[cbsa_geoid]['pop10'] = df['pop10'].sum()
		output_dict[cbsa_geoid]['Htu'] = Htu
		output_dict[cbsa_geoid]['Eu'] = Eu
	except:
		pass	

con.close()

cbsa_df = pd.DataFrame.from_dict(output_dict, orient='index')
cbsa_df = cbsa_df.dropna()

print cbsa_df.describe().round(2)
# print cbsa_df.head()
# print len(cbsa_df)
# df['Hdecomp'] = df['pop10'] * (Eu - df['Et'])
# cbsa_namedf.boxplot(column='Hdecomp', by='chg_cat', vert=False, grid=False)

fig = plt.figure()
w = 10
ax=cbsa_df.plot.scatter('tract_share', 'decomp', figsize=(w, w * .68), c='Htu', cmap='plasma_r')
# annotate points
for i, point in cbsa_df.iterrows():
	name = point['name'].split(',')[0]
	if "-" in name:
		name = name.split('-')[0]
	ax.text(point['tract_share'], point['decomp'], name , size=8, alpha=0.8)

ax.set_ylabel('between growth/loss tracts (4 groups) as % of metro H')
ax.set_xlabel('loss tracts as % of all metro tracts')
plt.savefig('/home/eric/Documents/franklin/fowler/figures/metro_between_tract_decomp.png')

plt.close()

# print cbsa_df.corr().round(2)


####################################################################################
fig = plt.figure()
w = 10
ax=cbsa_df.plot.scatter('Htu', 'decomp', figsize=(w, w * .68), c='tract_share', cmap='plasma_r')
# annotate points
for i, point in cbsa_df.iterrows():
	name = point['name'].split(',')[0]
	if "-" in name:
		name = name.split('-')[0]
	ax.text(point['Htu'], point['decomp'], name , size=8, alpha=0.8)

ax.set_ylabel('between growth/loss tracts (4 groups) as % of metro H')
ax.set_xlabel('total between-tract H')
plt.savefig('/home/eric/Documents/franklin/fowler/figures/metro_between_tract_decomp_v2.png')
plt.close()

# cbsa_df['decomp'].hist();plt.show()
# results = smf.ols('decomp ~ tract_share + Htu + np.log(pop10)', data=cbsa_df).fit()
# print results.summary()