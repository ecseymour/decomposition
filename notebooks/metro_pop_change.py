'''
what is population growth/loss status
of metros containing shrinking cities?
does decade matter?
look just at metros fitting criteria for
min number of places w/in metro + central city 50k+?
calc metro pop change based on "places" or total pop?
'''

import sqlite3 as sql
import pandas as pd
from matplotlib import pyplot as plt

db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
cur = con.cursor()

qry = '''
SELECT B.place10, C.CBSA, B.pop10, B.pop00,
(B.pop10 - B.pop00) * 1.0 / B.pop00 * 100 AS ppctchg0010_place,
(C.CL8AA2010 - C.CL8AA2000) * 1.0 / C.CL8AA2000 * 100 ppctchg0010_cbsa,
B.Ep AS city_entropy_2010
FROM shrinking_cities AS A JOIN subset_places_00_10 AS B
	ON A.NHGISCODE = B.nhgisplace10
JOIN nhgis_cbsa_data AS C ON 'G' ||A.cbsa_geoid10 = C.GISJOIN
;
'''

df = pd.read_sql(qry, con)
print df

fig = plt.figure()
w = 12
ax=df.plot.scatter('ppctchg0010_place', 'ppctchg0010_cbsa', figsize=(w, w*.681), c='city_entropy_2010', cmap='plasma_r')
for i, point in df.iterrows():
	name = point['place10'][:-5].strip()
	ax.text(point['ppctchg0010_place'], point['ppctchg0010_cbsa'], name , size=8, alpha=0.8, color='#525252')

plt.axhline(y=0.0, color='black', alpha=0.5, linestyle='--')
plt.axvline(x=0.0, color='black', alpha=0.5, linestyle='--')
ax.set_xlabel(r'city pop. % change 2000 - 2010')
ax.set_ylabel(r'metro pop. % change 2000 - 2010')

plt.savefig('/home/eric/Documents/franklin/fowler/figures/city_metro_pop_scatter.png')

con.close()