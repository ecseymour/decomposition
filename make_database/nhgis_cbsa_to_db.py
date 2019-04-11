import csv
import sqlite3 as sql

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
cur = con.cursor()

codebook = '/home/eric/Documents/franklin/fowler/data/nhgis/nhgis0088_csv/nhgis0088_ts_geog2010_cbsa_codebook.txt'

schema = []
field_only = []

with open(codebook, 'rb') as f:
	for line in f:
		if "Context Fields" in line:
			break
	for line in f:
		if "---" in line:
			break
		if line.startswith(' '*8):
			# print line				
			field_name = line.strip().split(":")[0]
			if any(i.isdigit() for i in field_name):
				field = (field_name, 'INT')
			else:
				field = (field_name, 'TEXT')
			field = ' '.join(field)
			if field_name != '':
				field_only.append(field_name)
				schema.append(field)
print schema

tablename = 'nhgis_cbsa_data'
cur.execute("DROP TABLE IF EXISTS {};".format(tablename))
cur.execute("CREATE TABLE IF NOT EXISTS {} ({});".format(tablename,  ', '.join(map(str, schema))))

# create insert template
cur.execute("SELECT * FROM {};".format(tablename))
fields = list([cn[0] for cn in cur.description])
qmarks = ["?"] * len(fields)
insert_tmpl = "INSERT INTO {} ({}) VALUES ({});".format(tablename, ', '.join(map(str, fields)),', '.join(map(str, qmarks)))
print insert_tmpl

datafile = '/home/eric/Documents/franklin/fowler/data/nhgis/nhgis0088_csv/nhgis0088_ts_geog2010_cbsa.csv'
with open(datafile, 'rb') as f:
	reader = csv.reader(f)
	header = reader.next()
	for row in reader:
		cur.execute(insert_tmpl,row)


cur.execute("CREATE INDEX idx_{}_gisjoin ON {}('GISJOIN');".format(tablename, tablename))

con.close()