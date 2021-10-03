import json

def lambda_handler(event, context):
    
    #print(event)
    
    GET_PATH = "/getPerson"
    CREATE_PATH = "/createPerson"
    
    if event["rawPath"] == GET_PATH:
        
        personID = event['queryStringParameters']['personID']
        
        # select * from person where personid=personID;
        
        return {
                "firstName": "adith",
                "lastName": "shetty",
                "nationality": "INDIAN"
        }
        
        
    elif event['rawPath'] == CREATE_PATH:
        
        pass
    
    # TODO implement
    '''
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    '''