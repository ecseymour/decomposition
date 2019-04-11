import csv
import sqlite3 as sql

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
cur = con.cursor()

codebook = '/home/eric/Documents/franklin/fowler/data/nhgis/nhgis0072_csv/nhgis0072_ts_geog2010_tract_codebook.txt'

schema = []

with open(codebook, 'rb') as f:
	for line in f:
		if "Context Fields" in line:
			break
	for line in f:
		if "Table 1" in line:
			break
		field_name = line.strip().split(":")[0]
		field = (field_name, 'TEXT')
		field = ' '.join(field)
		if field_name != '':
			schema.append(field)

# add table data to schema
with open(codebook, 'rb') as f:
	for line in f:
		if "Table 1:" in line:
			break
	for line in f:
		if "---" in line:
			break
		if "Time series" in line or "Table" in line:
			pass
		else:
			field_name = line.strip().split(":")[0]
			field = (field_name, 'INT')
			field = ' '.join(field)
			if field_name != '':
				schema.append(field)


tablename = 'nhgis_tract_data'
cur.execute("DROP TABLE IF EXISTS {};".format(tablename))
cur.execute("CREATE TABLE IF NOT EXISTS {} ({});".format(tablename,  ', '.join(map(str, schema))))

# create insert template
cur.execute("SELECT * FROM {};".format(tablename))
fields = list([cn[0] for cn in cur.description])
qmarks = ["?"] * len(fields)
insert_tmpl = "INSERT INTO {} ({}) VALUES ({});".format(tablename, ', '.join(map(str, fields)),', '.join(map(str, qmarks)))
print insert_tmpl

datafile = '/home/eric/Documents/franklin/fowler/data/nhgis/nhgis0072_csv/nhgis0072_ts_geog2010_tract.csv'
with open(datafile, 'rb') as f:
	reader = csv.reader(f)
	header = reader.next()
	for row in reader:
		cur.execute(insert_tmpl,row)

con.commit()
print "{} changes made".format(con.total_changes)

cur.execute('CREATE INDEX idx_tracts_gisjoin ON nhgis_tract_data(GISJOIN);')

con.close()