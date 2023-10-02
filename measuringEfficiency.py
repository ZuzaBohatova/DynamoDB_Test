import boto3 as bt
from boto3.dynamodb.conditions import Key, Attr
import time

def main():
    dynamodb = bt.resource('dynamodb')
    print("Testing the effectiveness of queries")
    print("--------------------------------------")
    print("returnAllPlayersOfNationality:")
    returnAverageTime(returnAllPlayersOfNationality, dynamodb, "CZE")
    measureTimeOfQuery(returnAllPlayersOfNationality, dynamodb, "CZE")
    print("playersScoringForTeamAway:")
    returnAverageTime(playersScoringForTeamAway, dynamodb, "Chelsea")
    measureTimeOfQuery(playersScoringForTeamAway, dynamodb, "Chelsea")
    print("averageAge:")
    returnAverageTime(averageAge,dynamodb)
    measureTimeOfQuery(averageAge,dynamodb)
    

def returnAverageTime(fce, *args):
    num_runs = 20
    total_time = 0
    for i in range(num_runs):
        start_time = time.time()
        fce(*args)
        end_time = time.time()
        total_time += (end_time - start_time)

    print("  Average query time of:",total_time/num_runs," seconds")

def measureTimeOfQuery(fce, *args):
    start_time = time.monotonic()
    fce(*args)
    end_time = time.monotonic()
    duration = end_time - start_time
    print(f"  One query duration: {duration:.5f} seconds")


def returnAllPlayersOfNationality(dynamodb, nationality):
    table = dynamodb.Table('Player')
    response = table.scan(
        FilterExpression=Attr('nationality').eq(nationality),
        ProjectionExpression='playerName, teamName'
    )

def playersScoringForTeamAway(dynamodb, teamName):
    table = dynamodb.Table('Match')

    response = table.query(
        IndexName='awayTeamIndex',
        KeyConditionExpression=Key('awayTeamName').eq(teamName)
    )
    matchIDs = [item['matchID'] for item in response['Items']]
    
    playerNames = []

    table = dynamodb.Table('PlayerMatch')
    for match_id in matchIDs:
        response = table.query(
            IndexName='matchIndex',
            KeyConditionExpression=Key('matchID').eq(match_id),
            ProjectionExpression='playerName '
        )
        playerNames.extend([item['playerName'] for item in response['Items']])

    table = dynamodb.Table("Player")
    teamPlayers = []
    for player in playerNames:
        response = table.query(
            IndexName='teamPlayerIndex',
            KeyConditionExpression=Key('teamName').eq(teamName)&Key("playerName").eq(player),
            ProjectionExpression='playerName, teamName'
        )
        teamPlayers.extend([item['playerName'] for item in response['Items']])
    player_counts = {}

    for player in teamPlayers:
        if player in player_counts:
            player_counts[player] += 1
        else:
            player_counts[player] = 1

    sorted(player_counts.items(), key=lambda x: x[1], reverse=True)

        

def averageAge(dynamodb):
    table = dynamodb.Table('Player')
    response = table.scan(
        ProjectionExpression='teamName, #age',
        ExpressionAttributeNames={
            '#age': 'age'
        }
    )

    teams = {}
    counts = {}

    for item in response['Items']:
        team = item['teamName']
        age = item['age']

        if team in teams:
            teams[team] += age
            counts[team] += 1
        else:
            teams[team] = age
            counts[team] = 1

    sorted(teams, key=lambda x: teams[x]/counts[x], reverse=True)
        

if __name__ == "__main__":
    main()

