import boto3
from botocore.retries import bucket
import glob2
import os
import pathlib

s3_client = boto3.client("s3", region_name="us-east-1") # Global Variables

s3_resource = boto3.resource("s3", region_name="us-east-1")  # Global Variables

var = 1

def create_bucket_aws(bucket_name):
    global s3_client # global keyword is used to keep the changes made to the 's3_client' variable
    
    try:

       s3_bucket = s3_client.create_bucket(Bucket = bucket_name)

       print("###########")
       print("S3 bucket created\n")
       print(s3_bucket)
       print("###########")

    except Exception as e:
        print("There was a problem while creating a bucket\n",e)

def list_buckets_aws():

    global s3_client

    bucket_list_object = s3_client.list_buckets() # Gives us a JSON result

    #print(bucket_list_object)
    for item in bucket_list_object['Buckets']:
        
        print(item['Name'])

def upload_file_aws(file_path, bucket, file_name = None):

    global s3_client

    if file_name is None:

        file_name_first_letter = file_path.rindex("\\") + 1  # Gives the index/position of s

        file_name = file_path[file_name_first_letter:]

    try:

        s3_client.upload_file(file_path, bucket, file_name)
        print("File Uploaded: ",file_name)

    except Exception as e:
        print("##########")
        print("Can't upload a folder or directory")
        print(e)
        print("##########")


def upload_folder(folder, bucket):

    for file_path in folder:

        upload_file_aws(file_path, bucket)  ###?


# Upload the entire AWS Services folder
def upload_entire_folder(path,bucket):
    global s3_client

    for root, dirs, files in os.walk(path):
        # print(root)
        # print(dirs)
        # print(files)
        # print("@#@#@#@#@#@#@#@#@#@#@")
        
        for file in files:
            full_path = os.path.join(root, file)
            with open(full_path, 'rb') as data:
                response=s3_client.put_object(Key=file,
                Bucket=bucket,
                Body=data)
                print(f"{file} uploaded sucessfully!")
                                  


def download_file_aws(file_name, bucket):

    global s3_resource

    folder_path = "C:\\Users\\15512\\OneDrive\\Desktop\\AWS services\\Download"

    if not os.path.exists(folder_path):

        os.makedirs(folder_path)

    file_object = s3_resource.Object(bucket, file_name)

    file_path = os.path.join(folder_path, file_name)

    file_object.download_file(file_path)


# Fetch & Download all the files present in AWS Services directory 
def download_many_files(files,bucket):
    files_only=[]
    for file_path in files:

        #print(file_path)
        file_name_start=file_path.rindex("\\")+1
        file_name=file_path[file_name_start:]        

        if os.path.isfile(file_name):
            files_only.append(file_name)
        
    for file in files_only:

        download_file_aws(file,bucket)
        print(f"{file} downloaded successfully!")

def delete_files_aws(bucket):
    global s3_client

    response=s3_client.delete_objects(
        Bucket=bucket,
        Delete={
            'Objects':[
                {'Key':"api.py"}, 
                {'Key':"cloudwatch.json"}, 
                {'Key':"moviedata.json"}, 
                {'Key':"MoviesCreateTableReference.py"}, 
                {'Key':"Notes.txt"}, 
                {'Key':"MyMoviesTable.py"}, 
                {'Key':"pandas_basics_practice (1).ipynb"}
            ]
        }
        
    )
    print("Files marked for deletion!")
    return response


if __name__ == "__main__":

    #create_bucket_aws("adiths3bucket")

    #list_buckets_aws()

    #upload_file_aws("C:\\Users\\15512\\OneDrive\\Desktop\\AWS services\\moviedata.json", "adiths3bucket")

    all_files = glob2.glob("C:\\Users\\15512\\OneDrive\\Desktop\\AWS services\\*")

    #upload_folder(all_files, "adiths3bucket")
    upload_entire_folder("C:\\Users\\15512\\OneDrive\\Desktop\\AWS services","adiths3bucket")

    #download_file_aws("musicdata.json","adiths3bucket")    
    #download_many_files("all_files","adiths3bucket")
    #print(delete_files_aws("adiths3bucket"))






