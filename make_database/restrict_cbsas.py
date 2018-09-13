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
#######################################################
# find CBSAs having at least 29 places in 1980

# create indexes
cur.execute("CREATE INDEX IF NOT EXISTS idx_cbsa_fips ON us_cbsa_2010(geoid10);")

years = ['1980', '1990', '2000', '2010']
for y in years:
	cur.execute("CREATE INDEX IF NOT EXISTS idx_places{}_gisjoin ON us_place_point_{}(gisjoin);".format(y, y))
	cur.execute("CREATE INDEX IF NOT EXISTS idx_nhgis_race_gjoin{} ON nhgis_race_place(GJOIN{});".format(y,y))

qry = '''
SELECT A.geoid10, A.name10, COUNT(*) AS tot_places
FROM us_cbsa_2010 AS A, us_place_point_1980 AS B
JOIN nhgis_race_place AS C 
	ON B.gisjoin = C.GJOIN1980
WHERE ST_Contains(A.geometry, B.geometry)
AND B.ROWID IN (SELECT ROWID FROM SpatialIndex 
	WHERE f_table_name='us_place_point_1980' AND search_frame=A.geometry)
AND B.place NOT LIKE "%CDP" AND C.B78AA1980 >= 1 AND C.B78AA1980 <> '' --exclude CDPs and places with pop < 1--
GROUP BY A.geoid10
HAVING COUNT(*) >= 29
'''
df = pd.read_sql(qry, con, index_col='geoid10')
print "CBSAs w/ 29+ places in 1980: {}".format(len(df))
#######################################################
# output ungrouped denver data for diagnostics
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
'''


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
#######################################################
# merge CBSAs w/ regions, then reduce to CBSAs for study
# start with centroids, reassign CBSAs later where necessary
qry = '''
SELECT B.geoid10, A.region , A.name AS region_name
FROM regions_2010 AS A, us_cbsa_2010 AS B
WHERE ST_Contains(A.geometry, ST_Centroid(B.geometry))
AND B.ROWID IN (SELECT ROWID FROM SpatialIndex
	WHERE f_table_name='us_cbsa_2010' AND search_frame=A.geometry)
;
'''
regions = pd.read_sql(qry, con, index_col='geoid10')
# merge/reduce
merged = pd.merge(merged, regions, left_index=True, right_index=True)
#######################################################
'''
using this subset of CBSAs, select all places located within them
existing in both 2000 and 2010 (later update to include all two-decade periods)
take spatial join of cbsa to 2010 places that join back to 2000 fips codes

note that reduces the 2000-2010 sample to 5406 places
CF had a sample of 7157 places, but that includes all 
places that existed for at least one census 1980-2010
2183 of these places lost pop 2000 to 2010
'''

qry = '''
SELECT A.geoid10 AS cbsa_geoid10, 
B.place AS place10,
B.nhgisplace AS nhgisplace10, 
B.gisjoin AS place_gisjoin10,
D.B78AA2000 AS pop00,
C.B78AA2010 AS pop10,
C.AE7AA2010 AS white10,
C.AE7AB2010 AS black10,
C.AE7AD2010 AS asian10,
C.AE7AG2010 + C.AE7AH2010 + C.AE7AI2010 + C.AE7AJ2010 + C.AE7AK2010 + C.AE7AL2010 AS hisp10,
C.AE7AC2010 + C.AE7AE2010 + C.AE7AF2010 AS other10
FROM us_cbsa_2010 AS A, us_place_point_2010 AS B --qry cbsa and place points tbls--
JOIN nhgis_race_place AS C 
	ON B.gisjoin = C.GJOIN2010 --join to race data using '10 gisjoin--
JOIN nhgis_race_place AS D
	ON B.gisjoin = D.GJOIN2000 --join to race data using '00 gisjoin--
WHERE ST_Contains(A.geometry, B.geometry) --cbsa contains 2010 place points--
AND B.ROWID IN (SELECT ROWID FROM SpatialIndex --use spat index--
	WHERE f_table_name='us_place_point_2010' AND search_frame=A.geometry)
AND B.place NOT LIKE "%CDP" --exclude CDPs from results-- 
AND C.B78AA2010 >= 1 AND C.B78AA2010 <> '' --where 2010 pop >= 1--
AND D.B78AA2000 >= 1 AND D.B78AA2000 <> '' --where 2000 pop >= 1--
;
'''
all_places = pd.read_sql(qry, con, index_col='place_gisjoin10')

# reduce all places to those inside one of our previously identified CBSAs
subset_places = pd.merge(merged, all_places, left_index=True, right_on='cbsa_geoid10')
print "subset places 2000-2010: {}".format(len(subset_places))
# reset index to place from cbsa
subset_places.set_index('nhgisplace10', inplace=True)
# write to db
subset_places.to_sql('subset_places_00_10', con, if_exists='replace')

#######################################################
'''
make table with places existing 2000 and 1990
need to change vars included in race categories
5319 places retained
'''

qry = '''
SELECT A.geoid10 AS cbsa_geoid10, 
A.name10 AS cbsa_name,
B.place AS place00,
B.nhgisplace AS nhgisplace00, 
B.gisjoin AS place_gisjoin00,
C.B78AA2000 AS pop00,
C.AE7AA2000 AS white00,
C.AE7AB2000 AS black00,
C.AE7AD2000 AS asian00,
C.AE7AG2000 + C.AE7AH2000 + C.AE7AI2000 + C.AE7AJ2000 + C.AE7AK2000 + C.AE7AL2000 AS hisp00,
C.AE7AC2000 + C.AE7AE2000 + C.AE7AF2000 AS other00,
D.B78AA1990 AS pop90
FROM us_cbsa_2010 AS A, us_place_point_2000 AS B --qry cbsa and place points tbls--
JOIN nhgis_race_place AS C 
	ON B.gisjoin = C.GJOIN2000 --join to race data using '00 gisjoin--
JOIN nhgis_race_place AS D
	ON B.gisjoin = D.GJOIN1990 --join to race data using '90 gisjoin--
WHERE ST_Contains(A.geometry, B.geometry) --cbsa contains 2000 place points--
AND B.ROWID IN (SELECT ROWID FROM SpatialIndex --use spat index--
	WHERE f_table_name='us_place_point_2000' AND search_frame=A.geometry)
AND B.place NOT LIKE "%CDP" --exclude CDPs from results-- 
AND C.B78AA2000 >= 1 AND C.B78AA2000 <> '' --where 2000 pop >= 1--
AND D.B78AA1990 >= 1 AND D.B78AA1990 <> '' --where 1990 pop >= 1--
;
'''
all_places = pd.read_sql(qry, con, index_col='place_gisjoin00')

# reduce all places to those inside one of our previously identified CBSAs
subset_places = pd.merge(merged, all_places, left_index=True, right_on='cbsa_geoid10')
print "subset places 1990-2000: {}".format(len(subset_places))
# reset index to place from cbsa
subset_places.set_index('nhgisplace00', inplace=True)
# write to db
subset_places.to_sql('subset_places_90_00', con, if_exists='replace')

#######################################################
'''
make table with places existing 1980 and 1990
GJOIN in NHGIS race table changes even for places
that continue to exist w/out change.
join on 1990 GJOIN, but restrict based on presence of 
1980 population values
'''

qry = '''
SELECT A.geoid10 AS cbsa_geoid10, 
A.name10 AS cbsa_name,
B.place AS place90,
B.nhgisplace AS nhgisplace90, 
B.gisjoin AS place_gisjoin90,
C.B78AA1990 AS pop90,
C.AE7AA1990 AS white90,
C.AE7AB1990 AS black90,
C.AE7AD1990 AS asian90,
C.AE7AG1990 + C.AE7AH1990 + C.AE7AI1990 + C.AE7AJ1990 + C.AE7AK1990 AS hisp90,
C.AE7AC2000 + C.AE7AE2000 AS other90,
C.B78AA1980 AS pop80
FROM us_cbsa_2010 AS A, us_place_point_1990 AS B --qry cbsa and place points tbls--
JOIN nhgis_race_place AS C 
	ON B.gisjoin = C.GJOIN1990
WHERE ST_Contains(A.geometry, B.geometry)
AND B.ROWID IN (SELECT ROWID FROM SpatialIndex --use spat index--
	WHERE f_table_name='us_place_point_1990' AND search_frame=A.geometry)
AND B.place NOT LIKE "%CDP" --exclude CDPs from results-- 
AND C.B78AA1990 >= 1 AND C.B78AA1990 <> '' --where 1990 pop >= 1--
AND C.B78AA1980 >= 1 AND C.B78AA1980 <> '' --where 1980 pop >= 1--
;
'''
all_places = pd.read_sql(qry, con, index_col='place_gisjoin90')

# reduce all places to those inside one of our previously identified CBSAs
subset_places = pd.merge(merged, all_places, left_index=True, right_on='cbsa_geoid10')
print "subset places 1980-1990: {}".format(len(subset_places))
# reset index to place from cbsa
subset_places.set_index('nhgisplace90', inplace=True)
# write to db
subset_places.to_sql('subset_places_80_90', con, if_exists='replace')



#######################################################
con.close()