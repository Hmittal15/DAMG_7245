import boto3
import os
import logging
from dotenv import load_dotenv
import sqlite3
import io
import re

load_dotenv()

# Define logging format
LOGLEVEL = os.environ.get('LOGLEVEL', 'INFO').upper()
logging.basicConfig(
    format='%(asctime)s %(levelname)-8s %(message)s',
    level=LOGLEVEL,
    datefmt='%Y-%m-%d %H:%M:%S')

# Establish connection to user bucket
s3client = boto3.client('s3', 
                        region_name = 'us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

def write_db_to_bucket():

    # Connect to the SQLite database
    conn = sqlite3.connect("filenames_nexrad.db")
    
    # Export the database to an SQL file
    with io.StringIO() as f:
        for line in conn.iterdump():
            f.write(line + "\n")
        f.seek(0)
        sql_content = f.read()

    # Upload the SQL file to the S3 bucket
    s3client.put_object(Bucket=os.environ.get('USER_BUCKET_NAME'), Key="database_nexrad.db", Body=sql_content.encode("utf-8"))

    # Clean up
    conn.close()

def create_metadata_nexrad():
    """Gathers data from the NEXRAD S3 bucket and creates metadata containing file paths for every files present 
    in that bucket in the form of a database"""
    logging.debug("fetching objects in NEXRAD s3 bucket")
    folders = ['2022/', '2023/']
    paginator = s3client.get_paginator('list_objects_v2')
    for year in folders:
        nexrad_bucket = paginator.paginate(Bucket='noaa-nexrad-level2', Prefix=year)
        conn = sqlite3.connect("filenames_nexrad.db")
        c = conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS filenames_nexrad (Year text, Month text, Day text, Station text, PKey text primary key)""")
        # c.execute("""DELETE FROM filenames_nexrad""")
        logging.info("Printing Files in NEXRAD bucket")
        for count, page in enumerate (nexrad_bucket):
            files = page.get("Contents")
            if (count%5 == 0):
                conn.commit()
            for file in files:
                print('\t' * 4 + file['Key'])
                filename = file['Key'].split('/')
                print(filename)
                pkey = "" + filename[0] + filename[1] + filename[2] + filename[3]
                c.execute("INSERT OR IGNORE INTO filenames_nexrad (Year , Month , Day , Station , PKey ) VALUES ('{}', '{}', '{}', '{}', '{}')".format(filename[0], filename[1], filename[2], filename[3], pkey))
    
    conn.commit()
    conn.close()

def read_metadata_nexrad():
    """Read the metadata from sqlite db"""
    station=set()
    year=set()
    month=set()
    day=set()
    db = sqlite3.connect("filenames_nexrad.db")
    cursor = db.cursor()
    meta_data=cursor.execute('''SELECT Station, Year , Month, Day FROM filenames_nexrad''')
    for record in meta_data:
        station.add(record[0])
        year.add(record[1])
        month.add(record[2])
        day.add(record[3])
    return station, year, month, day

def validate_file_nexrad(filename):
    """Validate if user provided a valid file name to get URL"""
    regex = re.compile('[@!#$%^&*()<>?/\|}{~:]')
    station, year, month, day= read_metadata_nexrad()
    count=0
    message=""
    x=filename.split("_")
    stat=x[0][:4]
    y=x[0][4:8]
    m=x[0][8:10]
    d=x[0][10:12]
    hh=x[1][:2]
    mm=x[1][2:4]
    ss=x[1][4:6]
    ext=x[-1][-3:]
    
    if(regex.search(filename) != None):
        count+=1
        message="Please avoid special character in filename\n"
    if (len(x[0])!=12):
        count+=1
        message+="Please provide station ID, valid date\n"
    if (stat not in station):
        count+=1
        message+="Please provide valid station ID\n"
    if (y not in year):
        count+=1
        message+="Please provide valid year\n"
    if (m not in month):
        count+=1
        message+="Please provide valid month\n"
    if (len(x[1])!=6):
        count+=1
        message="Please provide valid timestamp\n"
    if (hh>23):
        count+=1
        message+="Please provide valid hour\n"
    if (mm>59):
        count+=1
        message+="Please provide valid minutes\n"
    if (ss>59):
        count+=1
        message+="Please provide valid seconds\n"
    if (ext!='.gz'):
        count+=1
        message+="Please provide valid file extension\n"
    if (count==0):
        message="Valid file"
    print (message)

def list_files_in_user_bucket() :
    """Lists all the files present in the user's S3 bucket along with its file path"""
    logging.debug("fetching objects in user s3 bucket")
    my_bucket = s3client.list_objects(Bucket=os.environ.get('USER_BUCKET_NAME'))
    files = my_bucket.get('Contents')
    logging.info("Printing Files in User bucket")
    for file in files:
        print('\t' * 4 + file['Key'])

def list_files_in_nexrad_bucket():
    """Lists a few files present in the NEXRAD S3 bucket along with its file path"""
    logging.debug("fetching objects in NEXRAD s3 bucket")
    paginator = s3client.get_paginator('list_objects_v2')
    nexrad_bucket = paginator.paginate(Bucket='noaa-nexrad-level2', PaginationConfig={"PageSize": 2})
    logging.info("Printing Files in NEXRAD bucket")
    for count, page in enumerate(nexrad_bucket):
        files = page.get("Contents")
        for file in files:
            print('\t' * 4 + file['Key'])
        if count == 2:
            break

def main():
    # return
    list_files_in_user_bucket()
    # list_files_in_nexrad_bucket()
    # create_metadata_nexrad()
    # write_db_to_bucket()

if __name__ == "__main__":
    # logging.info("Script starts")
    main()
    # logging.info("Scripts ends")