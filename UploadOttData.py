#
# Python script for downloading, parsing, and saving values from
# the OTT Ecolog 500 pressure transducer at the outlet structure.
#
# Requires remote MySQL access to server.
#
# Made by: Nat Kale, PLSLWD Planner, 2012-06-20
# Updated with new benchmark: 2012-07-31

import os, sys, zipfile, logging, ConfigParser, xml.etree.ElementTree as xmlParser, MySQLdb as mdb
from ftplib import FTP
from datetime import datetime

#Some basic variables

# Config file needs to be in the same directory as the script; otherwise, alter configPath to match the location.
configPath = "ott_settings.ini"

siteid      = "PL_OUT"
mtypeid     = "ELEV"
benchmark   = 910.13
zeroElev    = 900
insertSQL   = " INSERT INTO `measurements` (`mtime`, `value`, `siteid`, `mtypeid`, `lab_id`) VALUES ('{0}', {1}, '{2}', '{3}', 'N/A') "
sensor      = ""
vals        = []
numFiles    = 0
numVals     = 0

# Logging format
logging.basicConfig(filename='UploadOttData.log',level=logging.DEBUG,format='%(asctime)s %(message)s')

config = ConfigParser.ConfigParser()
config.read(configPath)

#Set paths
downloadDir = config.get('filepaths','download_paths')
archiveDir = config.get('filepaths','archive_paths')

# Connect to FTP
ftp_dir     = config.get('ftp','url')
ftp_usr     = config.get('ftp','username')
ftp_pwd     = config.get('ftp','password')
target_dirs = config.get('ftp','target_dirs').split(",")


ftp = FTP(ftp_dir)
ftp.login(ftp_usr, ftp_pwd)

#Connect to MySQL
mysql_url   = config.get('mysql','url')
mysql_usr   = config.get('mysql','username')
mysql_pwd   = config.get('mysql','password')
mysql_db    = config.get('mysql','database')
mysql_ids   = config.get('mysql','siteids').split(",")

con = None
con = mdb.connect(mysql_url, mysql_usr, mysql_pwd, mysql_db);
cur = con.cursor()

ftp.cwd(target_dir)
files = ftp.nlst(target_dir)

#Grab the date/time of the most recent measurement recorded.
cur.execute("SELECT `mtime` FROM `measurements` WHERE `siteid` = 'PL_OUT' ORDER BY `mtime` DESC LIMIT 1 ")
if cur.rowcount > 0:
    lastMeasureDate = cur.fetchone()[0]
else:
    lastMeasureDate = datetime.strptime("19900101010000","%Y%m%d%H%M%S")
logging.info("Most recent data uploaded on: {0}".format(lastMeasureDate))

i = 0

#Open archive
archive = zipfile.ZipFile(archiveDir, "a")

# Grab data from files, & import to WQDB
for file in files:
    numFiles = numFiles + 1
    if file[0] != ".":
        fileName = file.split(target_dir)[1]
        logging.info(fileName)
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
                        cur.execute(sql)
                        con.commit()
                        numVals     = numVals + 1
            else:
                logging.info(file + " has no data.")
            
            #Archive the file, remove unnecessary copies.
            currfile.close()
            archive.write(downloadDir + fileName, fileName, zipfile.ZIP_DEFLATED)
            os.remove(downloadDir + fileName)
            ftp.delete(file)

logging.info("Read {0} files from FTP; added {1} values to the database.\n---\n".format(numFiles, numVals))

# Close connections
ftp.quit()
con.close()
