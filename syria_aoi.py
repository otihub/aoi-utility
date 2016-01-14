#!/usr/bin/env python 

import os, zipfile, pandas
from fiona import collection
from fiona.rfc3339 import parse_date
from shapely.geometry import shape, mapping
from datetime import date 
#DB connection properties

#user = str(raw_input("User name: "))
#paswd = str(raw_input("Password: "))
def unzip(source_filename, dest_dir):
	with zipfile.ZipFile(source_filename) as zf:
		for member in zf.infolist():
            # Path traversal defense copied from
            # http://hg.python.org/cpython/file/tip/Lib/http/server.py#l789
			words = member.filename.split('/')
			path = dest_dir
			for word in words[:-1]:
				drive, word = os.path.splitdrive(word)
				head, word = os.path.split(word)
				if word in (os.curdir, os.pardir, ''): continue
				path = os.path.join(path, word)
			zf.extract(member, path)

def recode(control):
	if control in ['Armed opposition groups and ANF','Non-state armed groups and ANF','Anti-ISIS Opposition Groups', 'Anti-ISIS (Moderate Opposition)']:
		return 1
	elif control in ['Contested','Contested Areas']:
		return 2
	elif control in ['Regime','Government (SAA)']:
		return 3
	elif control in ['ISIS-affiliated groups','ISIS']:
		return 4
	elif control in ['Kurdish', 'Kurdish Forces']:
		return 5 
	elif control in ['JAN']:
		return 6 
	else:
		return 7


def main():
	
#	Get OTI data as pandas data frame 
	
	oti_aoiname = 'oti_aoi.xlsx'
	otidata =  pandas.read_excel(os.path.join(os.getcwd(),'source/ignore/' + oti_aoiname), sheetname=0,header=0)
#	add code OTI data
	

#	Get sub regions shapefile with ogr

	with collection('source/sub_regions/Sub_Regions.shp', 'r') as input:
		schema = input.schema.copy()
		# declare that the new shapefile will have
		# new fields Date, Control.
		schema['properties']['Updated'] = 'str'
		schema['properties']['Control'] = 'str'
		with collection(
			'source/ignore/OTI_AOP.shp', 'w', 'ESRI Shapefile', schema) as output:
			for poly in input:
				print(int(poly['properties']['Nahia_Code']))
				try:
					otidata.loc[otidata['Nahia_Code'] == int(poly['properties']['Nahia_Code'])]
					
				# iterate through index and row in otidata 
				except KeyError:
							
					#match uid in otidata to poly
					print('keyerror')
					poly['properties']['Updated'] = 'NULL'
					poly['properties']['Control'] = 'NULL'
				else:
					print('matched')
					#update properties from otidata NULL might not be right
					poly['properties']['Updated'] = otidata.loc[otidata['Nahia_Code'] == int(poly['properties']['Nahia_Code'])]['Last_Update']
					poly['properties']['Updated'] = otidata.loc[otidata['Nahia_Code'] == int(poly['properties']['Nahia_Code'])]['Control_Status']

					
				output.write({
					'properties': poly['properties'],
					'geometry': mapping(shape(poly['geometry']))
				})
	#	Merge sub regions .shp with OTI data pandas frame


#	Export joined subregions w/ oti data to raster

#	Bring in ocha aoi
#		Extract from shp
#			Extract shp from mbd
	

#	add script to extract shp from mdb
	# ochashp = "ocha_aoi.zip"
	# ochazip = os.getcwd() + '/source/ignore/' + ochashp
#	os.system(os.getcwd() + "unzip " + ochashp + " -d " + os.getcwd())
	# unzip(ochazip, os.getcwd())


#	Recode ocha shp

#	Save ocha shp and raster

#	Burn ocha and oti rasters together with oti data taking preeminence


			
		

if __name__=="__main__":
	main()
