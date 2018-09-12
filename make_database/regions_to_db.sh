# set trunk for data dir
dir=/home/eric/Documents/franklin/fowler/data
dir2=/home/eric/Documents/franklin/fowler/data/region

# create spatialite db
ogr2ogr -f "SQLite" -update \
-t_srs http://spatialreference.org/ref/esri/102003/ \
"$dir/decomposition.sqlite" \
"$dir2/gz_2010_us_020_00_5m/gz_2010_us_020_00_5m.shp" \
-nlt PROMOTE_TO_MULTI -nln regions_2010