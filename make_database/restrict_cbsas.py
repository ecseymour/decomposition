'''
find CBSAs w/:
1) 29+ places 1980
2) 1+ place w/ pop >= 50k

exclude CDPs? 
'''

from pysqlite2 import dbapi2 as sql
import pandas as pd

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

# find CBSAs having at least 29 places in 1980
qry = '''
SELECT A.geoid10, A.name10, COUNT(*) AS tot_places
FROM us_cbsa_2010 AS A, us_place_point_1980 AS B
JOIN nhgis_race_place AS C 
	ON B.gisjoin = C.GJOIN1980
WHERE ST_Contains(A.geometry, B.geometry)
AND B.ROWID IN (SELECT ROWID FROM SpatialIndex 
	WHERE f_table_name='us_place_point_1980' AND search_frame=A.geometry)
AND B.place NOT LIKE "%CDP" AND C.B78AA1980 >= 1 AND C.B78AA1980 <> '' 
GROUP BY A.geoid10
HAVING COUNT(*) >= 29
'''
df = pd.read_sql(qry, con, index_col='geoid10')
print "CBSAs w/ 29+ places in 1980: {}".format(len(df))
#######################################################
# output ungrouped denver data for diagnostics
cur.execute("CREATE INDEX IF NOT EXISTS idx_places80_gisjoin ON us_place_point_1980(gisjoin);")
cur.execute("CREATE INDEX IF NOT EXISTS idx_nhgis_race_gjoin80 ON nhgis_race_place(GJOIN1980);")
qry = '''
SELECT B.*, C.B78AA1980, C.B78AA1990, C.B78AA2000, C.B78AA2010
FROM us_cbsa_2010 AS A, us_place_point_1980 AS B
JOIN nhgis_race_place AS C 
	ON B.gisjoin = C.GJOIN1980
WHERE ST_Contains(A.geometry, B.geometry)
AND B.ROWID IN (SELECT ROWID FROM SpatialIndex 
	WHERE f_table_name='us_place_point_1980' AND search_frame=A.geometry)
AND B.place NOT LIKE "%CDP"
AND A.geoid10 = '19740';
'''
denver = pd.read_sql(qry, con)
denver.to_csv("/home/eric/Documents/franklin/fowler/data/Denver_places_1980.csv", index=False)
#######################################################
'''
find CBSAs containing at least one principal city with pop >= 50k
note that some principal cities changed 1980 to 2009
so principal cities 2009 FIPS codes w/ drop some places we want to retain

join 2010 CBSA table to 2009 principal cities table to pop time-series table
join CBSA to principal cities on CBSA fips, selecting all principal cities,
then join to pop data and retain those w/ pop >= 50k in 1980
ADD INDEXES
'''

cur.execute("CREATE INDEX IF NOT EXISTS idx_cbsa_fips ON us_cbsa_2010(geoid10);")

qry = '''
SELECT A.geoid10, COUNT(*) AS tot_princip
FROM us_cbsa_2010 AS A JOIN principal_cities_2009 AS B
	ON A.geoid10 = B.cbsa_code
JOIN nhgis_race_place AS C
	ON B.place_fips = C.placea
WHERE C.B78AA1980 >= 50000 AND C.B78AA1980 <> ''
GROUP BY A.geoid10
HAVING COUNT(*) >= 1 
;
'''
df2 = pd.read_sql(qry, con, index_col='geoid10')
print "CBSAs w/ principal cities 50k+ in 1980: {}".format(len(df2))

merged = pd.merge(df, df2, left_index=True, right_index=True, how='inner')

print "CBSAs meeting both criteria: {}".format(len(merged))

merged.to_csv("/home/eric/Documents/franklin/fowler/data/CBSA_sample.csv", index_label='geoid10')


con.close()