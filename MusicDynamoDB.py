import json
import boto3
from decimal import Decimal
from pprint import pprint
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr


# Create Music Table
def create_music_table(dynamodb_client=None):

    if not dynamodb_client:
        dynamodb_client = boto3.client('dynamodb')

    response = dynamodb_client.create_table(
        TableName='Music',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH',
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE',
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'S',
            },
            {
                'AttributeName': 'title',
                'AttributeType': 'S',
            },
        ],
        
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10,
        }
        
    )
    return response

# Insert Items into the table
def load_music(music_list, dynamodb_client=None):
    if not dynamodb_client:
        dynamodb_client = boto3.client('dynamodb')

    for music in music_list:
        
        year = music['year'] if music['year'] else None
        music_name = music['title'] if music['title'] else None
        artist = music['artist'] if music['artist'] else None
        img_url = music['img_url'] if music['img_url'] else None
        web_url = music['web_url'] if music['web_url'] else None

        print("\n\n##############\n\n")
        print("Adding music:", music_name,end="\n\n")

        dynamodb_client.put_item(
            TableName='Music',
            Item={
                'year': {
                    'S': "{}".format(year),
                },
                'title':{
                    'S': "{}".format(music_name)
                },
                'artist':{
                    'S': "{}".format(artist)
                },
                'image_url':{
                    'S': "{}".format(img_url)
                },
                'web_url':{
                    'S': "{}".format(web_url)
                } 
            }
        )

# Retrieve data from table
def get_music(title, year, dynamodb_client=None):

    if not dynamodb_client:
        dynamodb_client = boto3.client('dynamodb')

    try:
        response = dynamodb_client.get_item(
            TableName='Music',
            Key={'year': {
                    "S": "{}".format(year)
                },
                 'title': {
                     "S":"{}".format(title)
                 }
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item'] 

# Update data in the table
def update_music(title, release_year,artist, dynamodb_client=None):
    if not dynamodb_client:
        dynamodb_client = boto3.client('dynamodb')

    

    response = dynamodb_client.update_item(
        TableName="Music",
        Key={
            'year': {
                "S": "{}".format(release_year),
            },
            'title': {
                "S": "{}".format(title)
            }
        },
        
        
        ExpressionAttributeNames={
            
            '#A': 'artist'
            
        },

        ExpressionAttributeValues={
            
            ':a': {
                "S": "{}".format(artist),
            }
           
        },
        UpdateExpression="SET #A= :a",
        ReturnValues="UPDATED_NEW"   ####?
    )
    return response


# Delete an Item from the table

def delete_music_item():
    
    dynamodb_client = boto3.resource('dynamodb')   ### region_name='ap-south-1')

    table = dynamodb_client.Table('Music')
    scan = table.query(
    KeyConditionExpression=Key('year').eq("1998")
    )
    with table.batch_writer() as batch:    ####?
        for item in scan['Items']:
            batch.delete_item(Key={'year':item['year'], 'title': item['title']})


# Delete the Movies table
def delete_music_table(dynamodb_client=None):
    if not dynamodb_client:
        dynamodb_client = boto3.resource('dynamodb')

    table = dynamodb_client.Table('Music')
    table.delete()


if __name__ == '__main__':

    # music_table = create_music_table()
    # print("Table status: ",music_table)

    # with open("musicdata.json") as json_file:
    #     music_list = json.load(json_file, parse_float=Decimal)
    # load_music(music_list)

    # music = get_music("Badfish", "1996",)
    # if music:
    #     print("Get Music succeeded:")
    #     pprint(music, sort_dicts=False)

    # update_response = update_music("Badfish", "1996","Sublime Band")
    # print("Update Music Succeeded:")
    # pprint(update_response, sort_dicts=False)

    # print("Attempting a conditional delete...")
    # delete_music_item()
    # print("Delete Music Item succeeded:")

    delete_music_table()




