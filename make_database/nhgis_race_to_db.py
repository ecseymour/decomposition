import csv
import sqlite3 as sql

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
cur = con.cursor()
#################################################################

field_only = []
schema = []
# add context fields to schema
codebook = "/home/eric/Documents/franklin/fowler/data/nhgis/nhgis0055_csv/nhgis0055_ts_nominal_place_codebook.txt"
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
			field_only.append(field_name)
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
				field_only.append(field_name)
				schema.append(field)

tablename = 'nhgis_race_place'
cur.execute("DROP TABLE IF EXISTS {};".format(tablename))
cur.execute("CREATE TABLE IF NOT EXISTS {} ({});".format(tablename,  ', '.join(map(str, schema))))

# create insert template
cur.execute("SELECT * FROM {};".format(tablename))
fields = list([cn[0] for cn in cur.description])
qmarks = ["?"] * len(fields)
insert_tmpl = "INSERT INTO {} ({}) VALUES ({});".format(tablename, ', '.join(map(str, fields)),', '.join(map(str, qmarks)))
print insert_tmpl
#################################################################
# insert data into newly created table
datafile = "/home/eric/Documents/franklin/fowler/data/nhgis/nhgis0055_csv/nhgis0055_ts_nominal_place.csv"
with open(datafile, 'rb') as f:
	reader = csv.reader(f)
	header = reader.next()
	for row in reader:
		cur.execute(insert_tmpl,row)

con.commit()

cur.execute("CREATE INDEX idx_nhgis_nhgiscode ON nhgis_race_place(NHGISCODE);")
cur.execute("CREATE INDEX idx_nhgis_placea ON nhgis_race_place(PLACEA);")

print "{} changes made".format(con.total_changes)


con.close()