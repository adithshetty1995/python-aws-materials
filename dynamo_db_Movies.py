import json
import boto3
from decimal import Decimal
from pprint import pprint
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr

# table creation
def create_movie_table(dynamodb_client=None):

    if not dynamodb_client:
        dynamodb_client = boto3.client('dynamodb')

    table = dynamodb_client.create_table(
        TableName='Movies',
        KeySchema=[
            {
                'AttributeName': 'year',
                'KeyType': 'HASH'  # Partition key
            },
            {
                'AttributeName': 'title',
                'KeyType': 'RANGE'  # Sort key
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'year',
                'AttributeType': 'N'
            },

            {
                'AttributeName': 'title',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    return table

# insert rows into table
def load_movies(movies, dynamodb_client=None):
    if not dynamodb_client:
        dynamodb_client = boto3.client('dynamodb')

    for movie in movies:
        
        year = movie['year'] if movie['year'] else None
        movie_name = movie['title'] if movie['title'] else None
        directors = ", ".join(movie['info']['directors']) if movie['info'].__contains__('directors')else None
        release_date = movie['info']['release_date'] if movie['info'].__contains__('release_date') else None
        
        genres = ", ".join(movie['info']['genres']) if movie['info'].__contains__('genres') else None
        image_url = movie['info']['image_url'] if movie['info'].__contains__('image_url') else None
        print("movie['info'].__contains__('plot'): ",movie['info'].__contains__('plot'))
        try:
            plot = movie['info']['plot']  if movie['info'].__contains__('plot') else None
        except:
            print("Movie: ",movie)
            break
        
        
        actors = ", ".join(movie['info']['actors']) if movie['info'].__contains__('actors') else None
        print("\n\n##############\n\n")
        print("Adding movie:", movie_name,end="\n\n")

        dynamodb_client.put_item(
            TableName='Movies',
            Item={
                'year': {
                    'N': "{}".format(year),
                },
                'title': {
                    'S': "{}".format(movie_name)
                },
                'directors': {
                    'S': "{}".format(directors)
                },
                'release_date': {
                    'S': "{}".format(release_date)
                },
                
                'genres': {
                    'S': "{}".format(genres)
                },
                'image_url': {
                    'S': "{}".format(image_url)
                },
                'plot': {
                    'S': "{}".format(plot)
                },
                
                'actors': {
                    'S': "{}".format(actors)
                },

            }
        )
# retrieve data from table
def get_movie(title, year, dynamodb_client=None):

    if not dynamodb_client:
        dynamodb_client = boto3.client('dynamodb')

    try:
        response = dynamodb_client.get_item(
            TableName='Movies',
            Key={'year': {
                    "N": "{}".format(year)
                },
                 'title': {
                     "S":"{}".format(title)
                 }
            }
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        return response['Item']   ####?


# Update data in the table
def update_movie(title, release_year,plot, actors, dynamodb_client=None):
    if not dynamodb_client:
        dynamodb_client = boto3.client('dynamodb')

    actors = ", ".join(actors)

    response = dynamodb_client.update_item(
        TableName="Movies",
        Key={
            'year': {
                "N": "{}".format(release_year),
            },
            'title': {
                "S": "{}".format(title)
            }
        },
        
        
        ExpressionAttributeNames={
            
            '#P': 'plot',
            '#A': 'actors'
            
        },

        ExpressionAttributeValues={
            
            ':p': {
                "S": "{}".format(plot),
            },
            ':a': {
                "S": "{}".format(actors),
            }   ####?
           
        },
        UpdateExpression="SET #P= :p, #A= :a",
        ReturnValues="UPDATED_NEW"   ####?
    )
    return response

# Delete an Item from the table
def delete_underrated_movie():
    
    dynamodb_client = boto3.resource('dynamodb')   ### region_name='ap-south-1')

    table = dynamodb_client.Table('Movies')
    scan = table.query(
    KeyConditionExpression=Key('year').eq(2013)
    )
    with table.batch_writer() as batch:    ####?
        for item in scan['Items']:
            batch.delete_item(Key={'year':item['year'], 'title': item['title']})

# Delete the Movies table
def delete_movie_table(dynamodb_client=None):
    if not dynamodb_client:
        dynamodb_client = boto3.resource('dynamodb')

    table = dynamodb_client.Table('Movies')
    table.delete()

        


if __name__ == '__main__':

    # movie_table = create_movie_table()
    # print("Table status: ",movie_table)

    # with open("moviedata.json") as json_file:
    #     movie_list = json.load(json_file, parse_float=Decimal)
    # load_movies(movie_list)

    # movie = get_movie("Gravity", 2013,)
    # if movie:
    #     print("Get movie succeeded:")
    #     pprint(movie, sort_dicts=False)  ####?

    # update_response = update_movie(
    #     "Gravity", 2013, 'A medical engineer and an astronaut work together to survive after an accident leaves them adrift in space.',
    #     ['Sandra Bullock', 'George Clooney'])
    # print("Update movie succeeded:")
    # pprint(update_response, sort_dicts=False)

    # print("Attempting a conditional delete...")
    # delete_underrated_movie()
    # print("Delete movie succeeded:")

    delete_movie_table()
    