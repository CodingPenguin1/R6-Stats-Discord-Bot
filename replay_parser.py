import subprocess
import pandas as pd
import json


client = None
roster_sheet = None


def parse_replay_directory(replay_dir):
    # === Run r6-dissect on extracted replay ===
    # For folder in match_dir, run r6-dissect
    replay_json = {}
    print(f'Running r6-dissect on {replay_dir}')
    replay_json = json.loads(subprocess.run(['./r6-dissect', replay_dir], capture_output=True).stdout.decode('utf-8'))

    # Sort rounds by ascending sum of scores
    replay_json['rounds'] = sorted(replay_json['rounds'], key=lambda x: x['teams'][0]['score'] + x['teams'][1]['score'])

    # === Generate stats dataframes from r6-dissect output ===
    # Player Stats
    print('Parsing player stats')
    player_df = parse_json_player_stats(replay_json)
    map_df = parse_map_stats(replay_json)
    return map_df, player_df

def parse_map_stats(replay_json):
    map_stats_df = pd.DataFrame(columns=[
        'Map',
        'Round Number',
        'Team 1',
        'Team 2',
        'Team 1 Score',
        'Team 2 Score',
        'ATK Team',
        'DEF Team',
        'Site',
        'Winning Team',
        'Winning Side',
        'Won by Objective'
    ])

    for round_ in replay_json['rounds']:
        if round_['matchFeedback'] is None:
            continue

        # Map
        map_ = round_['map']['name']

        # Round number
        round_num = round_['roundNumber'] + 1

        # Team names
        team_names = [round_['teams'][0]['name'], round_['teams'][1]['name']]

        # Scores
        team_1_score = round_['teams'][0]['score']
        team_2_score = round_['teams'][1]['score']

        # ATK and DEF teams
        atk_team = team_names[0] if round_['teams'][0]['role'] == 'Attack' else team_names[1]
        def_team = team_names[0] if round_['teams'][0]['role'] == 'Defend' else team_names[1]

        # Site played
        try:
            site = round_['site']
        except KeyError:
            site = 'N/A'

        # Winning team and side
        if round_['teams'][0]['won']:
            winning_team = team_names[0]
            winning_side = 'ATK' if round_['teams'][0]['role'] == 'Attack' else 'DEF'
        else:
            winning_team = team_names[1]
            winning_side = 'ATK' if round_['teams'][1]['role'] == 'Attack' else 'DEF'

        # Won by objective
        won_by_objective = False
        for event in round_['matchFeedback']:
            if event['type']['name'] == 'DefuserPlantComplete' or event['type']['name'] == 'DefuserDisableComplete':
                won_by_objective = True

        map_stats_df = pd.concat([map_stats_df, pd.DataFrame.from_records([{
            'Map': map_,
            'Round Number': round_num,
            'Team 1': team_names[0],
            'Team 2': team_names[1],
            'Team 1 Score': team_1_score,
            'Team 2 Score': team_2_score,
            'ATK Team': atk_team,
            'DEF Team': def_team,
            'Site': site,
            'Winning Team': winning_team,
            'Winning Side': winning_side,
            'Won by Objective': won_by_objective
        }])], ignore_index=True)

    return map_stats_df


def parse_json_player_stats(replay_json):
    player_df = pd.DataFrame(columns=['player', 'team', 'map', 'kills', 'deaths', 'assists', 'headshots', 'objectives', 'trades', 'opening kill', 'opening death', '2ks', '3ks', '4ks', 'aces', 'rounds', 'kost rounds', 'suicides', 'teamkills', '1vX'])

    # Stats from json stats section
    for player in replay_json['stats']:
        player_df = pd.concat([
            player_df,
            pd.DataFrame.from_records([{
                'player': player['username'],
                'rounds': player['rounds'],
                'kills': player['kills'],
                'deaths': player['deaths'],
                'assists': player['assists'],
                'headshots': player['headshots']
            }])
        ], ignore_index=True)

    # Player's teams
    players_teams = {}
    for player in replay_json['rounds'][0]['players']:
        username = player['username']
        team_index = player['teamIndex']
        team_name = replay_json['rounds'][0]['teams'][team_index]['name']
        players_teams[username] = team_name
    player_df = player_df.assign(team=player_df['player'].map(players_teams))

    # Map
    player_df['map'] = replay_json['rounds'][0]['map']['name']

    # Objective
    player_df['objectives'] = 0
    objective_log = []
    for round_num, round_ in enumerate(replay_json['rounds']):
        if round_['matchFeedback'] is None:  # Skip if no matchFeedback
            continue
        for event in round_['matchFeedback']:
            if event['type']['name'] == 'DefuserPlantComplete' or event['type']['name'] == 'DefuserDisableComplete' and (round_num, event['username']) not in objective_log:
                player = event['username']
                player_df.loc[player_df['player'] == player, 'objectives'] += 1
                objective_log.append((round_num, player))

    # Trades
    player_df['trades'] = 0
    kill_feed = []
    for round_ in replay_json['rounds']:
        round_kill_feed = []
        if round_['matchFeedback'] is None:  # Skip if no matchFeedback
            continue
        for event in round_['matchFeedback']:
            if event['type']['name'] == 'Kill':
                killer = event['username']
                killed = event['target']
                time = event['timeInSeconds']
                round_kill_feed.append((killer, killed, time))
        kill_feed.append(round_kill_feed)

    # Trade counts if someone kills someone who just got a kill within 10 seconds
    trade_log = []
    for round_num, round_kills in enumerate(kill_feed):
        for i in range(len(round_kills)):
            for j in range(i + 1, len(round_kills)):
                if round_kills[i][0] == round_kills[j][1] and abs(round_kills[j][2] - round_kills[i][2]) <= 10:
                    player_df.loc[player_df['player'] == round_kills[j][0], 'trades'] += 1
                    trade_log.append((round_num, round_kills[j][0]))

    # Opening kills
    player_df['opening kill'] = 0
    for round_kills in kill_feed:
        if len(round_kills):
            opening_kill = round_kills[0][0]
            opening_death = round_kills[0][1]
            player_df.loc[player_df['player'] == opening_kill, 'opening kill'] += 1
            player_df.loc[player_df['player'] == opening_death, 'opening death'] += 1

    # Opening death
    player_df['opening death'] = 0
    for round_ in replay_json['rounds']:
        if round_['matchFeedback'] is None:
            continue

        for event in round_['matchFeedback']:
            if event['type']['name'] == 'Kill':
                player_df.loc[player_df['player'] == event['target'], 'opening death'] += 1
                break
            elif event['type']['name'] == 'Death':
                player_df.loc[player_df['player'] == event['username'], 'opening death'] += 1
                break

    # 2k, 3k, 4k, ace
    player_df['2ks'] = 0
    player_df['3ks'] = 0
    player_df['4ks'] = 0
    player_df['aces'] = 0
    for round_ in replay_json['rounds']:
        for player in round_['stats']:
            if player['kills'] == 2:
                player_df.loc[player_df['player'] == player['username'], '2ks'] += 1
            elif player['kills'] == 3:
                player_df.loc[player_df['player'] == player['username'], '3ks'] += 1
            elif player['kills'] == 4:
                player_df.loc[player_df['player'] == player['username'], '4ks'] += 1
            elif player['kills'] == 5:
                player_df.loc[player_df['player'] == player['username'], 'aces'] += 1

    # KOST rounds
    player_df['kost rounds'] = 0
    for round_num in range(len(replay_json['rounds'])):
        for player in player_df['player'].values:
            survived, got_kill, got_trade, did_objective = False, False, False, False
            for player_stat in replay_json['rounds'][round_num]['stats']:
                if player_stat['username'] == player:
                    survived = not player_stat['died']
                    got_kill = player_stat['kills'] > 0
            for trade in trade_log:
                if trade[0] == round_num and trade[1] == player:
                    got_trade = True
            if replay_json['rounds'][round_num]['matchFeedback'] is None:  # Skip if no matchFeedback
                continue
            for event in replay_json['rounds'][round_num]['matchFeedback']:
                if event['username'] == player and event['type']['name'] == 'DefuserPlantComplete':
                    did_objective = True
            if survived or got_kill or got_trade or did_objective:
                player_df.loc[player_df['player'] == player, 'kost rounds'] += 1

    # Suicides
    player_df['suicides'] = 0
    for round_ in replay_json['rounds']:
        if round_['matchFeedback'] is None:  # Skip if no matchFeedback
            continue
        for event in round_['matchFeedback']:
            if event['type']['name'] == 'Death':
                player_df.loc[player_df['player'] == event['username'], 'suicides'] += 1

    # Teamkills
    player_df['teamkills'] = 0
    for round_num, round_feed in enumerate(kill_feed):
        for kill in round_feed:
            killer, target = kill[0], kill[1]

            killer_team_index = 0
            target_team_index = 0
            for player in replay_json['rounds'][round_num]['players']:
                if player['username'] == killer:
                    killer_team_index = player['teamIndex']
                elif player['username'] == target:
                    target_team_index = player['teamIndex']

            if killer_team_index == target_team_index:
                player_df.loc[player_df['player'] == killer, 'teamkills'] += 1

    # 1vX clutches
    player_df['1vX'] = 0
    for round_ in replay_json['rounds']:
        team_names = player_df['team'].unique()
        team_1_players = list(player_df[player_df['team'] == team_names[0]]['player'].values)
        team_2_players = list(player_df[player_df['team'] == team_names[1]]['player'].values)

        for player in round_['stats']:
            if player['died']:
                if player['username'] in team_1_players:
                    team_1_players.remove(player['username'])
                elif player['username'] in team_2_players:
                    team_2_players.remove(player['username'])

        # If only one left on your team and your team won the round
        if len(team_1_players) == 1:
            team_index = 0
            for player in round_['players']:
                if player['username'] == team_1_players[0]:
                    team_index = player['teamIndex']

            if round_['teams'][team_index]['won']:
                player_df.loc[player_df['player'] == team_1_players[0], '1vX'] += 1
        elif len(team_2_players) == 1:
            team_index = 0
            for player in round_['players']:
                if player['username'] == team_2_players[0]:
                    team_index = player['teamIndex']

            if round_['teams'][team_index]['won']:
                player_df.loc[player_df['player'] == team_2_players[0], '1vX'] += 1

    return player_df
