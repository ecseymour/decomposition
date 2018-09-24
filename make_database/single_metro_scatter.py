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

# pull data from table
# cbsa = '19820'; cbsa_name = 'Detroit metro' # detroit
cbsa = '17460'; cbsa_name = 'Cleveland metro'
# cbsa = '12060'; cbsa_name = 'Atlanta metro'
years = ['00', '10']
start = years[0]
end = years[1]
qry = "SELECT * FROM subset_places_{}_{} WHERE cbsa_geoid10 = {};".format(start, end, cbsa)
df = pd.read_sql(qry, con, index_col='nhgisplace10')
print "places in subset: {}".format(len(df))
############################################################
# calc metro-level segregation
############################################################
# sum race/ethnicity categories across places
cols = ['white{}'.format(end), 'black{}'.format(end), 'asian{}'.format(end), 'hisp{}'.format(end), 'other{}'.format(end)]
total_cbsa = pd.DataFrame(df[cols].sum(axis=0))
total_cbsa.columns = ['count']
# calc share of total in each category
total_cbsa['share'] = total_cbsa['count'] * 1.0 / total_cbsa['count'].sum(axis=0)
# calc entropy
Em = 0
for i, x in total_cbsa.iterrows():
	Em += x['share'] * np.log(1.0/x['share'])
# scale measure
Em = Em / np.log(len(cols))
print "Metro scaled entropy: {}".format(round(Em,3))
############################################################
# calc Hpm: compare each place to metro
############################################################
Tm = df['pop{}'.format(end)].sum(axis=0)
Hpm = 0
for i, x in df.iterrows():
	Hpm += x['pop{}'.format(end)] * (Em - x['Ep'])
Hpm = (1.0 / (Tm * Em)) * Hpm
print "Hpm: {}".format(round(Hpm,3))
############################################################
# for each place calc diff from metro E
############################################################
df['diff'] = 0
for i, x in df.iterrows():
	df.at[i, 'diff'] = x['pop{}'.format(end)] * (Em - x['Ep'])
	# print x['pop{}'.format(end)] * (Em - x['Ep'])
df['diff'] = df['diff'] * (1.0 / (Hpm * Tm))
print df[['diff']].head()
############################################################
# create groups based on pop change past decade
############################################################
df['chg{}{}'.format(start,end)] = (df['pop{}'.format(end)] - df['pop{}'.format(start)]) * 1.0 / df['pop{}'.format(start)] * 100
df['chg_cat'] = 0
df.loc[df['chg{}{}'.format(start,end)] < 0, 'chg_cat'] = 'loss'
df.loc[df['chg{}{}'.format(start,end)] >= 0, 'chg_cat'] = 'growth'
print "+" * 5
print "count in each group"
print df.groupby('chg_cat').size()

print df['diff'].describe()
# df['diff'].hist(bins=30);plt.show()
cols = ['place10', 'chg_cat', 'pwhite10', 'Ep', 'diff']
print df[cols].sort_values('diff', ascending=False).head(10)

print df[cols].sort_values('diff', ascending=True).head(10)

# df = df.drop(['G260220000'])
# df.plot.scatter('chg{}{}'.format(start,end), 'diff');plt.show()
ax=sns.boxplot(x=df['diff'], y=df['chg_cat'], order=['growth', 'loss'])
plt.title('{} (H = {})'.format(cbsa_name, round(Hpm,3)))
ax.set_ylabel('pop change {} to {}'.format(start, end))
ax.set_xlabel('place-level decomposition (scaled to metro)')
outF = "/home/eric/Documents/franklin/fowler/figures/{}Decomp.png".format(cbsa_name.split(' ')[0])
plt.savefig(outF)

con.close()