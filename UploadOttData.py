#
# Python script for downloading, parsing, and saving values from
# the OTT Ecolog 500 pressure transducer at the outlet structure.
#
# Requires remote MySQL access to server.
#
# Made by: Nat Kale, PLSLWD Planner, 2012-06-20
# Updated with new benchmark: 2012-07-31

import os, sys, zipfile, ConfigParser, xml.etree.ElementTree as xmlParser, MySQLdb as mdb
from ftplib import FTP
from datetime import datetime

#Some basic variables

#Directory for config file
# Switch to the commented out versions to use from a desktop, as opposed to the server (here and in settings.ini)

configPath  = "Z:/Class 509/655 - Monitoring & Data/Equipment/Ott Water Logger ecoLog500/settings.ini"
#configPath  = "D:/Office Files/Class 509/655 - Monitoring & Data/Equipment/Ott Water Logger ecoLog500/settings.ini"

siteid      = "PL_OUT"
mtypeid     = "ELEV"
benchmark   = 910.13
zeroElev    = 900
insertSQL   = " INSERT INTO `measurements` (`mtime`, `value`, `siteid`, `mtypeid`, `lab_id`) VALUES ('{0}', {1}, '{2}', '{3}', 'N/A') "
sensor      = ""
vals        = []
numFiles    = 0
numVals     = 0

config = ConfigParser.ConfigParser()
config.read('C:/Users/nkale/Desktop/GIT/settings.ini')

#Set paths
downloadDir = config.get('filepaths','download_path')
archiveDir = config.get('filepaths','archive_path')

# Connect to FTP
ftp_dir     = config.get('ftp','directory')
ftp_usr     = config.get('ftp','username')
ftp_pwd     = config.get('ftp','password')
target_dir  = config.get('ftp','target_dir')


ftp = FTP(ftp_dir)
ftp.login(ftp_usr, ftp_pwd)

ftp.cwd(target_dir)
files = ftp.nlst(target_dir)

#Connect to MySQL
mysql_url   = config.get('mysql','url')
mysql_usr   = config.get('mysql','username')
mysql_pwd   = config.get('mysql','password')
mysql_db    = config.get('mysql','database')

con = None
con = mdb.connect(mysql_url, mysql_usr, mysql_pwd, mysql_db);
cur = con.cursor()

#Grab the date/time of the most recent measurement recorded.
cur.execute("SELECT `mtime` FROM `measurements` WHERE `siteid` = 'PL_OUT' ORDER BY `mtime` DESC LIMIT 1 ")
if cur.rowcount > 0:
    lastMeasureDate = cur.fetchone()[0]
else:
    lastMeasureDate = datetime.strptime("19900101010000","%Y%m%d%H%M%S")
print "Most recent data uploaded on: {0}".format(lastMeasureDate)

#Open archive
archive = zipfile.ZipFile(archiveDir, "a")

# Grab data from files, & import to WQDB
for file in files:
    numFiles = numFiles + 1
    if file[0] != ".":
        fileName = file.split(target_dir)[1]
        print(fileName)
        filedatetime = datetime.strptime(fileName[11:25],"%Y%m%d%H%M%S")
        
        #Read out data in files not yet entered in the DB
        if filedatetime > lastMeasureDate :
            ftp.retrbinary("RETR " + file, open(downloadDir + fileName, 'wb').write)
            currfile    = open(downloadDir + fileName, 'r')
            contents    = currfile.read().strip()
            lines       = contents.split("\n")
            
            
            #Parse data, pulling just the elevation data & inserting into DB
            if len(lines) > 1:
                for line in lines:
                    if line[0] == "<":
                        fixedLine   = "<ln>" + line + "</ln>"
                        values      = xmlParser.fromstring(fixedLine)
                        sensor      = values.find("SENSOR").text
                    elif sensor == "0001":
                        vals.append(line.strip().split(":"))
                        theDate     = line.strip().split(";")[0]
                        theTime     = line.strip().split(";")[1]
                        val         = zeroElev + float(line.strip().split(";")[2])
                        dtime       = str(theDate) + str(theTime)
                        thedtime    = datetime.strptime(dtime, "%Y%m%d%H%M%S")
                        timeFormat  = thedtime.strftime("%Y-%m-%d %H:%M:%S")
                        sql         = insertSQL.format(timeFormat, val, siteid, mtypeid)
                        #print(sql)
                        cur.execute(sql)
                        con.commit()
                        numVals     = numVals + 1
            else:
                print(file + " has no data.")
            
            #Archive the file, remove unnecessary copies.
            currfile.close()
            archive.write(downloadDir + fileName, fileName, zipfile.ZIP_DEFLATED)
            os.remove(downloadDir + fileName)
            ftp.delete(file)

print("Read {0} files from FTP; added {1} values to the database.".format(numFiles, numVals))

# Close connections
ftp.quit()
con.close()
