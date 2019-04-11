'''
read cbsa to county xwalk to db
'''

import pandas as pd
import sqlite3 as sql

# connect to db
db = "/home/eric/Documents/franklin/fowler/data/decomposition.sqlite"
con = sql.connect(db)
con.text_factory=str
cur = con.cursor()
# read in excel w/ xwalk data
inFile = "/home/eric/Documents/franklin/fowler/data/county_cbsa_xwalk_2015.xls"
# keep/convert fips codes as text/string
converters = {
	'Metropolitan Division Code': str, 
	'CSA Code': str, 
	'FIPS State Code': str, 
	'FIPS County Code': str
	}
	
df = pd.read_excel(inFile, skiprows=2, skipfooter=4, converters=converters)
# replace spaces in col names w/ underscores
df.columns = [x.strip().replace(' ', '_') for x in df.columns]
df.columns = [x.strip().replace('/', '_') for x in df.columns]
# set CBSA code as index
df.index = df['CBSA_Code']
# keep all cols except CBSA code, now that it is stored as index
df = df.loc[:,'Metropolitan_Division_Code':]
# make custom 5 digit county fips code
df['fips5digit'] = df['FIPS_State_Code'] + df['FIPS_County_Code']
# print top 20 rows
print df.head(20)
# save to research db
df.to_sql('cbsa_county_xwalk_15', con, if_exists='replace')
# index fips code
cur.execute("CREATE INDEX idx_countyxwalk_fips ON cbsa_county_xwalk_15(fips5digit);")
con.close()