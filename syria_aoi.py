#!/usr/bin/env python 

import psycopg2, ppygis, os, pprint, xlrd, csv, zipfile, ogr, gdal, osr

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

def main():
		
	oti_aoiname = 'oti_aoi'
	fileName = 'source/' + oti_aoiname + '.xlsx'
	ExcelFile = os.path.join(os.getcwd(), fileName)
#	add script to extract shp from mdb
	ochashp = "ocha_aoi.zip"
	fullzip = os.getcwd() + '/source/' + ochashp
#	os.system(os.getcwd() + "unzip " + ochashp + " -d " + os.getcwd())
	unzip(fullzip, os.getcwd())
	
	with zipfile.ZipFile(fullzip) as zf:
		for member in zf.infolist():
			print(member.filename) 
			if str(member.filename)[-3:] == 'shp':
				shapefilename = str(member.filename)
				print('shapefilename: ' +  shapefilename)
				os.system("shp2pgsql -d -I -s 4326 "+ os.getcwd() + "/" + shapefilename + " OCHA.aoi | psql -d aoi")
				os.system("rm " + shapefilename[:-4] + ".*")
	
	
#	Excel2CSV(ExcelFile, 0) 
	conn = psycopg2.connect(dbname = 'aoi', host= 'localhost', port= 5432, user = 'ds', password='A3a3ello') # add this as raw input as well
	cur = conn.cursor()  ## open a cursor

	workbook = xlrd.open_workbook(ExcelFile)
	worksheet = workbook.sheet_by_index(0)
	pathcsv = os.path.join(os.getcwd(),'CSVFile.csv') # clean this up!!! upload from memory in python to excel
	csvfile = open(pathcsv, 'wb')
   	wr = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
	for rownum in xrange(worksheet.nrows):
		date = worksheet.row_values(rownum)[7] # change this to search through the header for the column with date in the name
		if isinstance( date, float) or isinstance( date, int ):
			year, month, day, hour, minute, sec = xlrd.xldate_as_tuple(date, workbook.datemode)
			py_date = "%02d/%02d/%04d" % (month, day,year)
			
			wr.writerow( list(x.encode('utf-8') if type(x) == type(u'') else x for x in  [worksheet.row_values(rownum)[i] for i in [2,6]]) + [py_date] )
		else:
			wr.writerow(
				list(x.encode('utf-8').strip() if type(x) == type(u'') else x.strip()
					for x in worksheet.row_values(rownum)))
	cur.execute("""DROP TABLE OTI.SRP_AOI""")
	cur.execute("""CREATE TABLE OTI.SRP_AOI(    
    	Nahia_Code integer,
   		Control_Status text,
   		Last_Update date
	);""")

	csvfile.close()
	copy_sql = """
           COPY oti.srp_aoi FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """
	with open(pathcsv, 'r') as f:
		cur.copy_expert(sql=copy_sql, file=f)
#		conn.commit() commits


#removes csv still needs to be reworked so csv never created i.e. load data direct from list.
	os.remove(pathcsv)
	cur.close()
	cur = conn.cursor()  ## open a cursor
	cur.execute("""DROP TABLE oti.aoiJOIN""")
	cur.execute("""CREATE TABLE oti.aoiJOIN AS
		SELECT 
    		sub.gid, 
    		CAST (sub.nahia_id as int) AS sub_nahia,
    		sub.mohafaza AS mohafaza,
    		sub.mantika AS mantika,
    		sub.nahia AS nahia,
    		srp.Nahia_Code AS nahia_id,
    		srp.Control_Status AS control,
    		srp.Last_Update AS updated,
    		sub.geom
		FROM ocha.syria_sub sub
		JOIN  oti.srp_aoi srp ON CAST (sub.nahia_id as int) = srp.Nahia_Code;""")

	cur.execute("""SELECT control FROM OTI.aoiJOIN LIMIT 10;""")
#	cur.execute("""SELECT current_user;""")
#	conn.commit()

			
	records = cur.fetchall()
	print(records)



if __name__=="__main__":
	main()
