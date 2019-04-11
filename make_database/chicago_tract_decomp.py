'''
test the degree to which tracts grouped 
into growth and loss categories accounts
for total between-tract segregation.
test w/ Chicago MSA for 2010 - 
identify tract loss based on 00-10 change.
pure loss - or below a threshold?

adapt script for calculating county E for tracts?

multiple loss categories? high and low loss?

separately look at black-white segregation?

compare across time: ideally keep tracts constant, 
but based on what criteria?
'''

from pysqlite2 import dbapi2 as sql
import pandas as pd
import numpy as np
from matplotlib import pyplot as plt

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

# get counties in Chicago metro

county_list = []

qry = '''
SELECT fips5digit
FROM cbsa_county_xwalk_15
-- WHERE CBSA_Code = '16980'; --chicago--
WHERE CBSA_Code = '19820'; --detroit--
-- WHERE CBSA_Code = '33340'; --mil--
'''
cur.execute(qry)
results = cur.fetchall()
for row in results:
	county_list.append(row[0])
print county_list

# get tracts for counties in metro
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
'''.format(tuple(county_list))
df = pd.read_sql(qry, con, index_col='GISJOIN')
con.close()

df['ppctchg'] = (df['pop10'] - df['pop00']) * 1.0 / df['pop00'] * 100
df['chg_cat'] = None
df.loc[df['ppctchg'] < -15, 'chg_cat'] = 'L2'
df.loc[(df['ppctchg'] < -5) & (df['ppctchg'] >= -15), 'chg_cat'] = 'L1'
df.loc[df['ppctchg'] >= -5, 'chg_cat'] = 'G1'
df.loc[df['ppctchg'] >= 10, 'chg_cat'] = 'G2'

# calc pct of each category
cols = ['white', 'black', 'asian', 'hisp', 'other']
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
print df.groupby('chg_cat').size()
print len(df)
df.to_csv('/home/eric/Documents/franklin/fowler/data/chicago_tract_diversity.csv')
###########################################################################################
# sum race/ethnicity categories across tracts
metro_total = pd.DataFrame(df[cols].sum(axis=0))
metro_total.columns = ['count']
# calc share of total in each category
metro_total['share'] = metro_total['count'] * 1.0 / metro_total['count'].sum(axis=0)
# calc entropy
Eu = 0
for i, x in metro_total.iterrows():
	Eu += x['share'] * np.log(1.0/x['share'])
# scale measure
Eu = Eu / np.log(len(cols))
print "metro scaled entropy: {}".format(round(Eu,3))
###########################################################################################
# calc Hru: compare each tract to metro
############################################################
Tu = df['pop00'].sum(axis=0)
Htu = 0
for i, x in df.iterrows():
	Htu += x['pop00'] * (Eu - x['Et'])
Htu = (1.0 / (Tu * Eu)) * Htu
print "Htu: {}".format(round(Htu,3))
###########################################################################################
# calc btw group H: loss/growth
###########################################################################################
for l in ['L2', 'L1', 'G1', 'G2']:
	print "+" * 40
	print l
	temp = pd.DataFrame(df.loc[df['chg_cat']==l][cols].sum(axis=0))
	temp.columns = ['count']
	# calc share of total in each category
	temp['share'] = temp['count'] * 1.0 / temp['count'].sum(axis=0)
	print temp
	# calc entropy
	Eg = 0
	for i, x in temp.iterrows():
		Eg += x['share'] * np.log(1.0/x['share'])
	# scale measure
	Eg = Eg / np.log(len(cols))
	print "metro {} group entropy: {}".format(l, round(Eg,3))
	# calc Hgu
	Tg = temp['count'].sum()
	Hgu = Tg * (Eu - Eg)
	Hgu = (1.0 / (Tu * Eu)) * Hgu
	print Hgu, (Hgu * 1.0 / Htu) * 100

# df.plot.scatter('ppctchg', 'Et');plt.show()