import boto3 as bt
from boto3.dynamodb.conditions import Key, Attr

def main():
    dynamodb = bt.resource('dynamodb')
    returnAllPlayersOfNationality(dynamodb, "CZE")
    playersScoringForTeamAway(dynamodb, "Chelsea")
    averageAge(dynamodb)

def returnAllPlayersOfNationality(dynamodb, nationality):
    table = dynamodb.Table('Player')
    response = table.scan(
        FilterExpression=Attr('nationality').eq(nationality),
        ProjectionExpression='playerName, teamName'
    )
    items = response['Items']
    print("Hráči ",nationality," národnosti: ")
    for item in items:
        print(f"   {item['playerName']} : {item['teamName']}")
    print()

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

    print("Hráči ",teamName,", kteří skórovali, když hrál tým venku:")
    for player, count in sorted(player_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {player}: {count}")

    print()
        

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

    print("Průměrný věk týmu:")
    for team in sorted(teams, key=lambda x: teams[x]/counts[x], reverse=True):
        average_age = teams[team] / counts[team]
        print(" ",team, ": ", round(average_age,3))

if __name__ == "__main__":
    main()