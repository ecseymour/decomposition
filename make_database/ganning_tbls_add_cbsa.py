from pysqlite2 import dbapi2 as sql

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

###############################################################################
# create metro column to shrinking cities table
cur.execute("ALTER TABLE shrinking_cities ADD COLUMN cbsa_geoid10 TEXT;")
cur.execute("ALTER TABLE shrinking_cities ADD COLUMN cbsa_name10 TEXT;")

qry = '''
SELECT DISTINCT B.geoid10, B.namelsad10, C.NHGISCODE 
FROM us_place_point_2010 AS A, us_cbsa_2010 AS B
JOIN shrinking_cities AS C ON A.nhgisplace = C.NHGISCODE 
WHERE ST_Contains(B.geometry, A.geometry)
AND A.ROWID IN (SELECT ROWID FROM SpatialIndex
	WHERE f_table_name='us_place_point_2010' AND search_frame = B.geometry)
;
'''
cur.execute(qry)
results = cur.fetchall()
for row in results:
	cur.execute("UPDATE shrinking_cities SET cbsa_geoid10 = ?, cbsa_name10 = ? WHERE NHGISCODE = ?;", (row[0], row[1], row[2]))
###############################################################################
cur.execute("ALTER TABLE all_cities_50k ADD COLUMN cbsa_geoid10 TEXT;")
cur.execute("ALTER TABLE all_cities_50k ADD COLUMN cbsa_name10 TEXT;")

qry = '''
SELECT DISTINCT B.geoid10, B.namelsad10, C.NHGISCODE 
FROM us_place_point_2010 AS A, us_cbsa_2010 AS B
JOIN all_cities_50k AS C ON A.nhgisplace = C.NHGISCODE 
WHERE ST_Contains(B.geometry, A.geometry)
AND A.ROWID IN (SELECT ROWID FROM SpatialIndex
	WHERE f_table_name='us_place_point_2010' AND search_frame = B.geometry)
;
'''
cur.execute(qry)
results = cur.fetchall()
for row in results:
	cur.execute("UPDATE all_cities_50k SET cbsa_geoid10 = ?, cbsa_name10 = ? WHERE NHGISCODE = ?;", (row[0], row[1], row[2]))


con.commit()
con.close()
print 'done'