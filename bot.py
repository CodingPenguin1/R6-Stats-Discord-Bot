import os
import discord
from discord import app_commands
from dotenv import load_dotenv
import pandas as pd
from zipfile import ZipFile
from replay_parser import parse_replay_directory


load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


@client.event
async def on_ready():
    await tree.sync()

    # Set status to idle, "awaiting replay file"
    await client.change_presence(activity=discord.Game(name='awaiting replay file'), status=discord.Status.idle)


@tree.command(
    name='parse',
    description='Upload a zip file of match replays you wish to parse with this command'
)
async def parse_replay(interaction, file: discord.Attachment):
    print('Parse command triggered')

    # Set status to active, "parsing replays"
    await client.change_presence(activity=discord.Game(name='parsing replays'), status=discord.Status.online)

    # Defer interaction to allow for processing time
    await interaction.response.defer()

    # Check if file is a zip file
    if not file.filename.endswith('.zip'):
        print(f'Invalid file type: {file.filename}')
        await interaction.response.send_message('Please upload a zip file')
        return

    # Save the zip file
    with open(file.filename, 'wb') as f:
        print(f'Saving {file.filename}')
        await file.save(os.path.join('input_buffer', file.filename))

    # Delete the file from the root directory
    os.remove(file.filename)

    # Extract the zip file to cache/
    with ZipFile(os.path.join('input_buffer', file.filename), 'r') as zip_ref:
        print(f'Extracting {file.filename} to cache/')
        zip_ref.extractall('cache')

    # Delete the zip file
    os.remove(os.path.join('input_buffer', file.filename))

    # Find all directories in cache/ that contain .rec files
    replay_directories = []
    for root, dirs, files in os.walk('cache'):
        if any(file.endswith('.rec') for file in files):
            replay_directories.append(root)
    replay_directories.sort()

    print(replay_directories)

    # Parse each replay
    map_dfs, player_dfs = [], []
    for replay in replay_directories:
        print(f'Parsing {replay}')
        map_df, player_df = parse_replay_directory(replay)
        map_dfs.append(map_df)
        player_dfs.append(player_df)

    # Concatenate the dataframes
    map_df = pd.concat(map_dfs)
    player_df = pd.concat(player_dfs)

    # Delete the cached replays
    for replay in replay_directories:
        print(f'Deleting {replay}')
        for root, dirs, files in os.walk(replay):
            for file in files:
                os.remove(os.path.join(root, file))
        os.rmdir(replay)

    # Wait for the replays to be parsed before sending the stats
    print('Waiting for stats to be generated')
    while len(map_dfs) < len(replay_directories):
        pass
    print('Stats generated')

    # Save dataframes to file
    map_df.to_csv('map_stats.csv', index=False)
    player_df.to_csv('player_stats.csv', index=False)

    # Send files
    await interaction.followup.send('', files=[discord.File('map_stats.csv'), discord.File('player_stats.csv')])

    # Delete the files
    os.remove('map_stats.csv')
    os.remove('player_stats.csv')

    # Set status back to default
    await client.change_presence(activity=discord.Game(name='awaiting replay file'), status=discord.Status.idle)


def main():
    client.run(TOKEN)


if __name__ == '__main__':
    main()
