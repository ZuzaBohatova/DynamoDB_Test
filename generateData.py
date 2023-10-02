import boto3 as bt
import csv
import time

def main():
    dynamodb = bt.resource('dynamodb')
    create_player(dynamodb)
    create_team(dynamodb)
    create_stadium(dynamodb)
    create_match(dynamodb)
    create_playerMatch(dynamodb)
    

def create_player(dynamodb):
    players_table = create_playersTable(dynamodb)
    insert_playerData(players_table, "./players.csv")

def create_team(dynamodb):
    team_table = create_teamTable(dynamodb)
    insert_teamData(team_table, "./teams.csv")

def create_stadium(dynamodb):
    stadium_table = create_stadiumTable(dynamodb)
    insert_stadiumData(stadium_table, "./stadiums.csv")

def create_match(dynamodb):
    matches_table = create_matchesTable(dynamodb)
    insert_matchesData(matches_table, "./matches.csv")

def create_playerMatch(dynamodb):
    playerMatch_table = create_playerMatchTable(dynamodb)
    insert_playerMatchData(playerMatch_table, "./playerMatch.csv")

def ifTableExistsDelete(dynamodb, table_name):
    for table in dynamodb.tables.all():
        if(table.name == table_name):
            table.delete()
            time.sleep(5)
 

def create_playersTable(dynamodb):
    ifTableExistsDelete(dynamodb, 'Player')
    table = dynamodb.create_table(
    TableName='Player',
    KeySchema=[
        {
            'AttributeName': 'playerName',
            'KeyType': 'HASH'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'playerName',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'age',
            'AttributeType': 'N'
        },
        {
            'AttributeName': 'teamName',
            'AttributeType': 'S'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    },
    GlobalSecondaryIndexes=[
        {
            'IndexName': 'ageIndex',
            'KeySchema': [
                {
                    'AttributeName': 'age',
                    'KeyType': 'HASH'
                }
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        },
        {
            'IndexName': 'teamPlayerIndex',
            'KeySchema': [
                {
                    'AttributeName': 'teamName',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'playerName',
                    'KeyType': 'RANGE'
                }
            ],
            'Projection': {
                'ProjectionType': 'ALL'
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 10,
                'WriteCapacityUnits': 10
            }
        }

    ]
)
    
    table.wait_until_exists()
    print("Table PLAYER created.")
    return table

def insert_playerData(table, file_path: str):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
    
        for row in reader:
            position = row['Position'].split(",")
            with table.batch_writer() as batch:
                batch.put_item(
                    Item={
                        'playerName': row['Name'],
                        'teamName': row['Club'],
                        'nationality': row['Nationality'],
                        'position': position,    
                        'age': int(row['Age']),
                        'goals': int(row['Goals']),
                    }
                )
    print("Data INSERTED into the PLAYER table.")

def create_teamTable(dynamodb):
    ifTableExistsDelete(dynamodb, 'Team')
    table = dynamodb.create_table(
        TableName='Team',
        KeySchema=[        
            {            
                'AttributeName': 'teamName',
                'KeyType': 'HASH'        }  
        ],
        AttributeDefinitions=[        
            {            
                'AttributeName': 'teamName',            
                'AttributeType': 'S'        
            },        
            {            
                'AttributeName': 'ranking',
                'AttributeType': 'N'        
            }    
        ],
        GlobalSecondaryIndexes=[        
            {            
                'IndexName': 'rankingIndex',
                'KeySchema': [                
                    { 
                        'AttributeName': 'ranking',
                        'KeyType': 'HASH'                
                    }            
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    
    table.wait_until_exists()
    print("Table TEAM created.")
    return table

def insert_teamData(table, file_path: str):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
    
        for row in reader:
            with table.batch_writer() as batch:
                batch.put_item(
                    Item={
                        'teamName': row['Name'],
                        'ranking': int(row['Ranking']),
                        'numberOfWins': int(row['NumberOfWins']),
                        'numberOfDraws': int(row['NumberOfDraws']),
                        'numberOfLosses': int(row['NumberOfLosses']),
                        'points': int(row['Points']),
                        'stadiumName': row['stadiumName']
                    }
                )
    print("Data INSERTED into the TEAM table.")
    
def create_stadiumTable(dynamodb):
    ifTableExistsDelete(dynamodb, 'Stadium')
    table = dynamodb.create_table(
        TableName='Stadium',
        KeySchema=[        
            {            
                'AttributeName': 'stadiumName',
                'KeyType': 'HASH'        }  
        ],
        AttributeDefinitions=[        
            {            
                'AttributeName': 'stadiumName',            
                'AttributeType': 'S'        
            } 
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    
    table.wait_until_exists()
    print("Table STADIUM created.")
    return table

def insert_stadiumData(table, file_path: str):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
    
        for row in reader:
            with table.batch_writer() as batch:
                batch.put_item(
                    Item={
                        'stadiumName': row['stadiumName'],
                        'capacity': int(row['capacity']),
                        'address':{
                            'street': row['street'],
                            'city': row['city'],
                            'postalCode': row['postalCode'],
                            'country': row['country']
                        }                                
                    }
                )
    print("Data INSERTED into the STADIUM table.")       


def create_matchesTable(dynamodb):
    ifTableExistsDelete(dynamodb, 'Match')
    table = dynamodb.create_table(
        TableName='Match',
        KeySchema=[        
            {            
                'AttributeName': 'matchID',
                'KeyType': 'HASH'        
            }    
        ],
        AttributeDefinitions=[       
            {            
                'AttributeName': 'matchID',
                'AttributeType': 'N'        
            },        
            {            
                'AttributeName': 'awayTeamName',
                'AttributeType': 'S'        
            }    
        ],
        GlobalSecondaryIndexes=[        
            {            
                'IndexName': 'awayTeamIndex',
                'KeySchema': [
                    {                    
                        'AttributeName': 'awayTeamName', 
                        'KeyType': 'HASH'                
                    }            
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 5,
                    'WriteCapacityUnits': 5
                }
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )

    
    table.wait_until_exists()
    print("Table MATCH created.")
    return table

def insert_matchesData(table, file_path: str):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
    
        for matchID, row in enumerate(reader):
                           
            with table.batch_writer() as batch:
                batch.put_item(
                    Item={
                        'matchID': matchID,
                        'date': row['Date'],
                        'time': row['Time'],
                        'homeTeamName': row['HomeTeam'],    
                        'awayTeamName': row['AwayTeam'],
                        'goalHomeTeam': int(row['FTHG']),
                        'goalAwayTeam': int(row['FTAG'])                        
                    }
                )
    print("Data INSERTED into the MATCH table.")

def create_stadiumTable(dynamodb):
    ifTableExistsDelete(dynamodb, 'Stadium')
    table = dynamodb.create_table(
        TableName='Stadium',
        KeySchema=[        
            {            
                'AttributeName': 'stadiumName',
                'KeyType': 'HASH'        
            }  
        ],
        AttributeDefinitions=[        
            {            
                'AttributeName': 'stadiumName',            
                'AttributeType': 'S'        
            } 
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    
    table.wait_until_exists()
    print("Table STADIUM created.")
    return table

def insert_stadiumData(table, file_path: str):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
    
        for row in reader:
            with table.batch_writer() as batch:
                batch.put_item(
                    Item={
                        'stadiumName': row['stadiumName'],
                        'capacity': int(row['capacity']),
                        'address':{
                            'street': row['street'],
                            'city': row['city'],
                            'postalCode': row['postalCode'],
                            'country': row['country']
                        }                                
                    }
                )
    print("Data INSERTED into the STADIUM table.")       


def create_playerMatchTable(dynamodb):
    ifTableExistsDelete(dynamodb, 'PlayerMatch')
    table = dynamodb.create_table(
        TableName='PlayerMatch',
        KeySchema=[
            {
                'AttributeName': 'playerMatchID',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'playerMatchID',
                'AttributeType': 'N'
            },
            {
                'AttributeName': 'matchID',
                'AttributeType': 'N'
            }
        ],
        GlobalSecondaryIndexes=[        
            {            
                'IndexName': 'matchIndex',
                'KeySchema': [
                    { 
                        'AttributeName': 'matchID',
                        'KeyType': 'HASH'                
                    }    
                ],
                'Projection': {
                    'ProjectionType': 'ALL'
                },
                'ProvisionedThroughput': {
                    'ReadCapacityUnits': 10,
                    'WriteCapacityUnits': 10
                }
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        }
    )
    
    table.wait_until_exists()
    print("Table PLAYERMATCH created.")
    return table

def insert_playerMatchData(table, file_path: str):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for playerMatchID, row in enumerate(reader):
                           
            with table.batch_writer() as batch:
                batch.put_item(
                    Item={
                        'playerMatchID': playerMatchID,
                        'playerName': row['PlayerName'],
                        'matchID': int(row['MatchID'])                      
                    }
                )
    print("Data INSERTED into the PLAYERMATCH table.")


if __name__ == "__main__":
    main()