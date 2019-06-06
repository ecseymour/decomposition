'''
create diversity profile (at 2010) 
comparing the npc's where the aggregate result is loss 
and the npcs where the aggregate result is gain
'''

from pysqlite2 import dbapi2 as sql
import pandas as pd
from matplotlib import pyplot as plt
import numpy as np
from collections import OrderedDict
import seaborn as sns

# pull info on aggregate loss/growth for NPCs in each loss metro
df = pd.read_csv('/home/eric/Documents/franklin/fowler/scripts/analysis/loss_metro_data.csv')
df = df.loc[df['level_1']=='NPC']
df.index=df['level_0']
print df.head()

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

shrinking_metros = OrderedDict()
qry = '''
SELECT B.cbsa_geoid10, A.CBSA, SUM(B.pop90), SUM(C.pop10)
FROM nhgis_cbsa_data AS A
JOIN subset_places_90_00 AS B
ON A.GISJOIN = "G"||B.cbsa_geoid10
JOIN subset_places_00_10 AS C
ON A.GISJOIN = "G"||C.cbsa_geoid10
GROUP BY A.CBSA
HAVING SUM(B.pop90) > SUM(C.pop10)
;
'''
results = cur.execute(qry)
for row in results:
	shrinking_metros[row[0]] = {'PC': {}, 'NPC': {} }

'''
iterate through metros, select sample places,
find non-principal cities, select diversity 
and plot profiles
'''

print len(shrinking_metros)

# 13 shrinking metros: 4 x 7 plot?
fig, axes = plt.subplots(nrows=4, ncols=4, sharex=True, sharey=True, figsize=(16,16), squeeze=False)
axli = axes.flatten()

count = 0
for k, v in shrinking_metros.iteritems():
	if count>=13:
		pass
	else:	
		qry = '''
		SELECT B.nhgisplace10, B.name10, B.Ep,
		CASE 
			WHEN C.place_fips IS NOT NULL THEN 1
			ELSE 0
		END princip
		FROM subset_places_80_90 AS A
		JOIN subset_places_00_10 AS B
			ON A.nhgisplace90 = B.nhgisplace10 --requires places exist in each decade from 1980 to 2010 (too strict?)--
		LEFT JOIN principal_cities_2009 AS C 
			ON A.nhgisplace90 = "G" || C.state_fips || "0" || C.place_fips || "0"
		WHERE A.cbsa_geoid10 = '{}'
		AND C.place_fips IS NULL;
		'''.format(k)
		temp = pd.read_sql(qry, con, index_col='nhgisplace10')
		ax=sns.kdeplot(data=temp['Ep'], label='all', ax=axli[count], legend=False)

		cbsa_name = temp['name10'].str.split(',')[0][0]
		
		if df.loc[int(k)]['pop_chg_binary'] == 'G':
			ax.set_title(cbsa_name, color='red')
		else:
			ax.set_title(cbsa_name)
		ax.title.set_size(10)

	count+=1

# remove unused subplots
for i in range(13, 16):
	fig.delaxes(axes.flatten()[i])

plt.suptitle('Shrinking NPCs in red', color='red', size=20)

outFile = "/home/eric/Documents/franklin/fowler/figures/npc_diversity_profiles_2010.jpg"
plt.savefig(outFile, bbox_inches='tight')
