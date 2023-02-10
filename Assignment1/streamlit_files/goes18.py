import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from dotenv import load_dotenv
import boto3
import os
import botocore
import re
import time


st.markdown("<h1 style='text-align: center;'>GOES-18</h1>", unsafe_allow_html=True)
st.header("")
st.header("Search by Fields")
st.header("")




#Generating logs with given message in cloudwatch
def write_logs(message : str):
    clientlogs.put_log_events(
    logGroupName = "assignment1-logs",
    logStreamName = "goes-logs",
    logEvents = [
        {
            'timestamp' : int(time.time() * 1e3),
            'message' : message
        }
    ]                            
    )





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
        message="Please avoid special character in filename"
    elif (x[0]!='OR'):
        count+=1
        message="Please provide valid prefix for Operational system real-time data"
    elif (prod_name not in prod):
        count+=1
        message="Please provide valid product name"
    elif ((goes!='G16') and (goes!='G18')):
        count+=1
        message="Please provide valid satellite ID"
    elif ((start[0]!='s') or (len(start)!=15) or (start[1:5] not in year) or (start[5:8] not in day) or (start[8:10] not in hour)):
        count+=1
        message="Please provide valid start date"
    elif ((end[0]!='e') or (len(end)!=15)):
        count+=1
        message="Please provide valid end date"
    elif ((create[0][0]!='c') or (len(create[0])!=15)):
        count+=1
        message="Please provide valid create date"
    elif (x[-1][-3:]!='.nc'):
        count+=1
        message="Please provide valid file extension"
    elif (count==0):
        message="Valid file"
    return (message)


def copy_to_public_bucket(src_bucket_name, src_object_key, dest_bucket_name, dest_object_key):
    copy_source = {
        'Bucket': src_bucket_name,
        'Key': src_object_key
    }
    s3client.copy_object(Bucket=dest_bucket_name, CopySource=copy_source, Key=dest_object_key)


def generate_download_link(bucket_name, object_key, expiration=3600):
    response = s3client.generate_presigned_url(
        ClientMethod='get_object',
        Params={
            'Bucket': bucket_name,
            'Key': object_key
        },
        ExpiresIn=expiration
    )
    write_logs(f"{[object_key.rsplit('/', 1)[-1],response]}")
    return response



def path_from_filename(filename):

    ind = filename.index('s')
    file_path = f"ABI-L1b-RadC/{filename[ind+1: ind+5]}/{filename[ind+5: ind+8]}/{filename[ind+8: ind+10]}/{filename}"
    return file_path



def check_if_file_exists_in_s3_bucket(bucket_name, file_name):
    try:
        s3client.head_object(Bucket=bucket_name, Key=file_name)
        return True

    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        else:
            raise
    # except s3client.exceptions.NoSuchKey:
    #     return False


load_dotenv()


s3client = boto3.client('s3', 
                        region_name = 'us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

#Establish connection to logs
clientlogs = boto3.client('logs', 
                        region_name = 'us-east-1',
                        aws_access_key_id = os.environ.get('AWS_ACCESS_KEY'),
                        aws_secret_access_key = os.environ.get('AWS_SECRET_KEY')
                        )

conn = sqlite3.connect("filenames_goes.db")
c = conn.cursor()


col1, col2, col3 = st.columns(3, gap="large")


query = c.execute("SELECT DISTINCT Year FROM filenames_goes")
year_list = [row[0] for row in query]


with col1:
    year = st.selectbox(
        'Select the Year :',
        (year_list))
    st.write('You selected :', year)


query = "SELECT DISTINCT Day FROM filenames_goes where Year = ?"
result = c.execute(query, (year,))
day_list = [row[0] for row in result]


with col2:
    day = st.selectbox(
        'Select the Day :',
        (day_list))
    st.write('You selected :', day)
 

query = "SELECT DISTINCT Hour FROM filenames_goes where Year=? and Day=?"
result = c.execute(query, (year, day,))
hour_list = [row[0] for row in result]


with col3:
    hour = st.selectbox(
        'Select the Hour :',
        (hour_list))
    st.write('You selected :', hour)
st.header("")


result = s3client.list_objects(Bucket='noaa-goes18', Prefix=f"ABI-L1b-RadC/{year}/{day}/{hour}/")
file_list = []
files = result.get("Contents", [])
for file in files:
    file_list.append(file["Key"].split('/')[-1])


selected_file = st.selectbox("Select link for download", 
            (file_list),  
            key=None, help="select link for download")


goes18_bucket = 'noaa-goes18'
selected_object_key = f'ABI-L1b-RadC/{year}/{day}/{hour}/{selected_file}'
user_bucket_name = os.environ.get('USER_BUCKET_NAME')
user_object_key = f'logs/goes18/{selected_file}'

st.header("")

if st.button('Generate using Filter'):    
    copy_to_public_bucket(goes18_bucket, selected_object_key, user_bucket_name, user_object_key)
    download_link = generate_download_link(user_bucket_name, user_object_key)
    st.write('Download Link : ', download_link.split("?")[0])
    split_object=selected_object_key.split('/')
    print("https://noaa-goes18.s3.amazonaws.com/index.html#"+split_object[0]+"/"+split_object[1]+"/"+split_object[2]+"/"+split_object[3]+"/")


st.header("")
st.header("Search by Filename")
st.header("")


filename = st.text_input('Enter Filename')
st.header("")


if st.button('Generate using Name'): 
    if (filename == ""): 
        st.write("Please enter file name")

    else:
        try:
            file_integrity = validate_file(filename) 
            if (file_integrity == 'Valid file') :
                if ('s' in filename):
                    selected_object_key = path_from_filename(filename)
                    file_exists = check_if_file_exists_in_s3_bucket(goes18_bucket, selected_object_key)
                else:
                    file_exists = False

                try:
                    if file_exists:
                        copy_to_public_bucket(goes18_bucket, selected_object_key, user_bucket_name, user_object_key)
                        download_link = generate_download_link(user_bucket_name, user_object_key)
                        st.write('Download Link : ', download_link.split("?")[0])
                    else:
                        raise Exception("File Not Found")
                except Exception as e:
                    st.write("File Not Found")
            else:
                st.write(file_integrity)
        except Exception as ee:
            st.write("Invalid filename")

            