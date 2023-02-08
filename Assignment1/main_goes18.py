import boto3
import os
import logging
from dotenv import load_dotenv
import sqlite3
import io
import pandas as pd
import re

load_dotenv()

# Generating URL from filename
def getURL(filename):
    x=filename.split("_")
    goes=x[2][1:]
    prod=x[1].split("-")
    if prod[2][-1].isdigit():
        prod[2]=prod[2][:-1]
    year=x[3][1:5]
    day=x[3][5:8]
    hour=x[3][8:10]
    print("https://noaa-goes"+goes+".s3.amazonaws.com/"+prod[0]+"-"+prod[1]+"-"+prod[2]+"/"+year+"/"+day+"/"+hour+"/"+filename)

# Define logging format
LOGLEVEL = os.environ.get ('LOGLEVEL', 'INFO') .upper()
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
    conn = sqlite3.connect("filenames_goes.db")
    

    # Export the database to an SQL file
    with io.StringIO() as f:
        for line in conn.iterdump():
            f.write(line + "\n")
        f.seek(0)
        sql_content = f.read()

    # Upload the SQL file to the S3 bucket
    s3client.put_object(Bucket=os.environ.get('USER_BUCKET_NAME'), Key="database_goes.db", Body=sql_content.encode("utf-8"))

    # Clean up
    conn.close()

def create_metadata_noaa():
    """Gathers data from the GOES18 S3 bucket and creates metadata containing file paths for every files present 
    in that bucket in the form of a database"""
    logging.debug("fetching objects in NOAA s3 bucket")
    paginator = s3client.get_paginator('list_objects_v2')
    noaa_bucket = paginator.paginate(Bucket='noaa-goes18', Prefix="ABI-L1b-RadC")
    conn = sqlite3.connect("filenames_goes.db")
    c = conn.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS filenames_goes (Product text, Year text, Day text, Hour text, PKey text primary key)""")
    c.execute("""DELETE FROM filenames_goes""")
    logging.info("Printing Files in NOAA bucket")
    for count, page in enumerate (noaa_bucket):
        files = page.get("Contents")
        if (count%5 == 0):
            conn.commit()
        for file in files:
            print('\t' * 4 + file['Key'])
            filename = file['Key'].split('/')
            print(filename)
            pkey = "" + filename[0] + filename[1] + filename[2] + filename[3] 
            c.execute("INSERT OR IGNORE INTO filenames_goes (Product , Year , Day , Hour, PKey) VALUES ('{}', '{}', '{}', '{}', '{}')".format(filename[0], filename[1], filename[2], filename[3], pkey))
    
    conn.commit()
    conn.close()

def read_metadata_noaa():
    """Read the metadata from sqlite db"""
    prod=set()
    year=set()
    day=set()
    hour=set()
    db = sqlite3.connect("filenames_goes.db")
    cursor = db.cursor()
    meta_data=cursor.execute('''SELECT Product , Year , Day , Hour FROM filenames_goes''')
    for record in meta_data:
        prod.add(record[0])
        year.add(record[1])
        day.add(record[2])
        hour.add(record[3])
    return prod, year, day, hour

def validate_file(filename):
    """Validate if user provided a valid file name to get URL"""
    regex = re.compile('[@!#$%^&*()<>?/\|}{~:]')
    prod, year, day, hour= read_metadata_noaa()
    count=0
    message=""
    x=filename.split("_")
    goes=x[2]
    my_prod=x[1].split("-")
    prod_name=my_prod[0]+"-"+my_prod[1]+"-"+my_prod[2]
    start=x[3]
    end=x[4]
    create=x[5].split(".")
    
    if(regex.search(filename) != None):
        count+=1
        message="Please avoid special character in filename\n"
    if (x[0]!='OR'):
        count+=1
        message+="Please provide valid prefix for Operational system real-time data\n"
    if (prod_name not in prod):
        count+=1
        message+="Please provide valid product name\n"
    if ((goes!='G16') and (goes!='G18')):
        count+=1
        message+="Please provide valid satellite ID\n"
    if ((start[0]!='s') or (len(start)!=15) or (start[1:5] not in year) or (start[5:8] not in day) or (start[8:10] not in hour)):
        count+=1
        message+="Please provide valid start date\n"
    if ((end[0]!='e') or (len(end)!=15)):
        count+=1
        message+="Please provide valid end date\n"
    if ((create[0][0]!='c') or (len(create[0])!=15)):
        count+=1
        message+="Please provide valid create date\n"
    if (x[-1][-3:]!='.nc'):
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

def list_files_in_noaa_bucket():
    """Lists a few files present in the GOES18 S3 bucket along with its file path"""
    logging.debug("fetching objects in NOAA s3 bucket")
    paginator = s3client.get_paginator('list_objects_v2')
    noaa_bucket = paginator.paginate(Bucket='noaa-goes18', PaginationConfig={"PageSize": 2})
    logging.info("Printing Files in NOAA bucket")
    for count, page in enumerate(noaa_bucket):
        files = page.get("Contents")
        for file in files:
            print('\t' * 4 + file['Key'])
        if count == 2:
            break

def main():
    # return
    # list_files_in_user_bucket()
    list_files_in_noaa_bucket()
    getURL("OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc")
    # create_metadata_noaa()
    # write_db_to_bucket()

if __name__ == "__main__":
    # logging.info("Script starts")
    main()
    # logging.info("Scripts ends")