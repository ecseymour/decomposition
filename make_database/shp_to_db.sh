# set trunk for data dir
dir=/home/eric/Documents/franklin/fowler/data
dir2=/home/eric/Documents/franklin/fowler/data/nhgis/nhgis0055_shape

# create spatialite db
ogr2ogr -f "SQLite" -dsco SPATIALITE=YES \
-t_srs http://spatialreference.org/ref/esri/102003/ \
"$dir/decomposition.sqlite" \
"$dir2/nhgis0055_shapefile_tl2010_us_cbsa_2010/US_cbsa_2010.shp" \
-nlt PROMOTE_TO_MULTI

# loop over places and add to db
for i in 1980 1990 2000 2010
do
	echo  "$dir2/nhgis0055_shapefile_tlgnis_us_place_point_$i"
	ogr2ogr -f "SQLite" -update \
	-t_srs http://spatialreference.org/ref/esri/102003/ \
	"$dir/decomposition.sqlite" \
	"$dir2/nhgis0055_shapefile_tlgnis_us_place_point_$i/US_place_point_$i.shp"
done