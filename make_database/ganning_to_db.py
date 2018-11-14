from pysqlite2 import dbapi2 as sql
import pandas as pd

# connect to research db
# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
con.enable_load_extension(True)
con.execute('SELECT load_extension("mod_spatialite");')
cur = con.cursor()

# pull ganning table into df
src = '/home/eric/Documents/franklin/fowler/data/Ganning_Supplementary_File.xlsx'

dtypes = {'PlaceFIPS': str}

df = pd.read_excel(src, sheet_name='Shrinking Cities', skiprows=1, dtype=dtypes)
print df.head()
# add trailing 0 missing from ganning's table
df['NHGISCODE'] = df['NHGISCODE'] + '0'
df.index = df['NHGISCODE']
df.drop(['NHGISCODE'], axis=1, inplace=True)
# read to db table
df.columns = df.columns.str.strip()
df.columns = df.columns.str.replace(' ', '')
df.columns = df.columns.str.replace('.', '_')
df.to_sql('shrinking_cities', con, if_exists='replace', index=True)
print "+" * 80
print "shrinking cities"
print df.head()
################################################################################
################################################################################
# all cities
df = pd.read_excel(src, sheet_name='Full Data (50k+)', skiprows=1, dtype=dtypes)
df['NHGISCODE'] = df['NHGISCODE'] + '0'
df.index = df['NHGISCODE']
df.drop(['NHGISCODE'], axis=1, inplace=True)
# read to db table
df.columns = df.columns.str.strip()
df.columns = df.columns.str.replace(' ', '')
df.columns = df.columns.str.replace('.', '_')
df.to_sql('all_cities_50k', con, if_exists='replace', index=True)
print "+" * 80
print "all cities"
print df.head()
################################################################################
################################################################################
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
con.commit()
################################################################################
################################################################################
con.close()
print 'done'