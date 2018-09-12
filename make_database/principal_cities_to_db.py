import sqlite3 as sql
import re

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
cur = con.cursor()

# create db table
cur.execute("DROP TABLE IF EXISTS principal_cities_2009;")

cur.execute('''
	CREATE TABLE principal_cities_2009 (
	cbsa_code TEXT,
	city_name TEXT,
	state_fips TEXT,
	place_fips TEXT
	);
	''')

tablename = 'principal_cities_2009'
cur.execute("SELECT * FROM {};".format(tablename))
fields = list([cn[0] for cn in cur.description])
qmarks = ["?"] * len(fields)
insert_tmpl = "INSERT INTO {} ({}) VALUES ({});".format(tablename, ', '.join(map(str, fields)),', '.join(map(str, qmarks)))


inFile = "/home/eric/Documents/franklin/fowler/data/princip09.txt"
with open(inFile, 'rb') as f:
	for line in f:
		# capture lines containing a FIPS code at end of line
		m = re.search(r'\d{5}$', line)
		if m:
			# break at spaces and strip space
			line = [l.strip() for l in line.split("   ") if l.strip() != '']
			try:
				cur.execute(insert_tmpl, line)
			except:
				print line

con.commit()
print "total changes: {}".format(con.total_changes)

cur.execute("CREATE INDEX idx_princip_cbsa ON principal_cities_2009(cbsa_code);")
cur.execute("CREATE INDEX idx_princip_place ON principal_cities_2009(place_fips);")

con.close()