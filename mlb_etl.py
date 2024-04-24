from datetime import datetime, timedelta
import pandas as pd
import random, string, os

from lib.utils import (
    GAMES_URL,
    TEAM_ADDRESS_URL,
    WEATHER_API_URL,
    make_api_call,
    camel_to_snake,
    upload_to_s3
)


start_date = (datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d')
end_date = (datetime.today() + timedelta(days=7)).strftime('%Y-%m-%d')
print('start date:', start_date)
print('end date:', end_date)


def get_mlb_data(start_date, end_date):
    """
    Takes two future dates and calls the mlb api data end point and fetches MLB
    games with a start date between those two dates.
    
    Flattens the nested json and transforms into a dataframe.
    
    """
    
    games_api_url = GAMES_URL
    results = []

    games_api_url = games_api_url.format(start_date=start_date, end_date=end_date)
    games_data = make_api_call(games_api_url)
    
    if games_data:
        print('successfully downloaded MLB games data')
        for dt in games_data['dates']:
            for game in dt['games']:
                flattened_game = game.copy()
                
                # flatten 'status', 'venue' and 'content' keys
                for category in ['status', 'venue', 'content']:
                    [flattened_game.update({category + '_' + key: game[category][key]}) for key, value in game[category].items()]

                # flatten teams info
                flattened_game.update({
                    'home_team_id': str(game['teams']['home']['team']['id']),
                    'home_team_name': game['teams']['home']['team']['name'],
                    'home_team_link': game['teams']['home']['team']['link'],
                    'home_team_wins': game['teams']['home']['leagueRecord']['wins'],
                    'home_team_losses': game['teams']['home']['leagueRecord']['losses'],
                    'home_team_win_rate': float(game['teams']['home']['leagueRecord']['pct']),

                    'away_team_id': str(game['teams']['away']['team']['id']),
                    'away_team_name': game['teams']['away']['team']['name'],
                    'away_team_link': game['teams']['away']['team']['link'],
                    'away_team_wins': game['teams']['away']['leagueRecord']['wins'],
                    'away_team_losses': game['teams']['away']['leagueRecord']['losses'],
                    'away_team_win_rate': float(game['teams']['away']['leagueRecord']['pct']),
                })
                
                # remove flattened json fields
                del flattened_game['status']
                del flattened_game['teams']
                del flattened_game['venue']
                del flattened_game['content']
                
                results = results + [flattened_game]
    else:
        quit()
    
    return pd.DataFrame(results)


def clean_mlb_data(games_df):
    """
    Takes the raw dataframe of MLB games.
    
    - Transforms the column names
    - Fixes data types (IDs --> string, dates --> datetime64)
    - Adds a new timestamp called 'time_zero_minute' which is the same as 'game_date' timestamp except
      that miuntes and smaller units are set to zero (will be used for merging with weather data).
      
    """
    
    # column names
    games_df.columns = [camel_to_snake(col) for col in games_df.columns]
    
    # id fields
    games_df.rename(columns={'game_pk': 'game_id'}, inplace=True)
    games_df['game_id'] = games_df.game_id.astype('str')
    games_df['venue_id'] = games_df.venue_id.astype('str')
    
    # date fields
    games_df['game_date'] = pd.to_datetime(games_df.game_date)
    games_df['time_zero_minute'] = games_df.game_date.dt.tz_localize(None).apply(
        lambda x: x.replace(minute=0, second=0, microsecond=0)
    )
    games_df['official_date'] = pd.to_datetime(games_df.official_date)
    
    return games_df


def get_venue_coordinates():
    """
    Gets MLB stadium data from https://gist.github.com/the55/2155142
    Cleans the data (adds missing teams and updates team names).
    
    """
    
    team_address_url = TEAM_ADDRESS_URL
    team_address_list = make_api_call(team_address_url)
    if not team_address_list:
        quit()
    
    # add missing teams to the list
    team_address_list = team_address_list + [
        {
        'team': 'Miami Marlins',
        'address': '501 Marlins Way, Miami, FL 33125',
        'lat': 25.7781487,
        'lng':-80.2221747,
        },
        {
        'team': 'Los Angeles Angels',
        'address': '2000 E Gene Autry Way, Anaheim, CA 92806',
        'lat': 33.7998135,
        'lng':-117.8824162,
        },
    ]
    
    team_address_df = pd.DataFrame(team_address_list).drop('address', axis=1)
    
    # old team names correction
    team_address_df.loc[team_address_df['team'] == 'Cleveland Indians', 'team'] = 'Cleveland Guardians'
    team_address_df.loc[team_address_df['team'] == 'Tampa Bay Devil Rays', 'team'] = 'Tampa Bay Rays'
    
    print('successfully downloaded MLB venues coordinates')
    
    return team_address_df


def get_weather_data(games_df):
    """
    Takes the MLB games data. Uses the stadium coordinates to call the open meteo
    end point and get the 9-day weather forecast for each location.
    
    9-day window is picked over the default 7-day to ensure fetching the weather data
    for all games.
    
    """
    
    weather_url = WEATHER_API_URL
    weather_df = pd.DataFrame()

    for ix, row in games_df[['lat', 'lng']].drop_duplicates().iterrows():
        lat = row['lat']
        lng = row['lng']
        weather_location_url = weather_url.format(lat=lat, lng=lng)
        weather_data = make_api_call(weather_location_url)
        
        if weather_data:
            weather_location_df = pd.DataFrame(weather_data['hourly'])
            weather_location_df['time'] = pd.to_datetime(weather_location_df.time)
            weather_location_df.rename(columns={'time': 'time_zero_minute'}, inplace=True)
            weather_location_df['lat'] = lat
            weather_location_df['lng'] = lng

            weather_df = pd.concat([weather_df, weather_location_df])
        else:
            quit()

    print('successfully downloaded weather data')
    return weather_df


def drop_pii_and_extra_fields(games_df):
    """
    Takes the MLB games dataset and removes PII fields along with extra fields that
    were created to join the games data to weather data.
    
    """
    
    games_df.drop(
        labels=[
            'game_guid',
            'reverse_home_away_status',
            'inning_break_length',
            'time_zero_minute',
            'lat',
            'lng',
        ],
        axis=1,
        inplace=True
    )
    
    return games_df


def add_random_id(games_df, random_seed=8675309):
    """
    Takes the MLB games dataset and a random_seed.
    Creates a 12-alphanumeric random ID and adds it as a column.
    Creates another column that checks if there is '19' in the random IDs.
    
    """
    
    random.seed(random_seed)
    
    random_id_list = [''.join(random.choices(string.ascii_letters + string.digits, k=12)) for i in range(len(games_df))]
    games_df['random_id'] = random_id_list
    games_df['id_includes_nineteen'] = games_df.random_id.apply(lambda x: int('19' in x))
    
    return games_df


def add_jenny(games_df, random_seed=8675309):
    """
    Takes the MLB games dataset and a random_seed.
    Randomly assigns rows a number between -150 to 150
    Creates an error column (error means jenny <= -50)
    
    """
    
    random.seed(random_seed)
    
    jenny_list = [random.random()*300 - 150 for i in range(len(games_df))]
    games_df['jenny'] = jenny_list
    games_df['jenny_error'] = games_df.jenny.apply(lambda x: x <= -50)
    
    return games_df


def run_etl(start_date, end_date):
    """
    Run the entire ETL by calling all the functions defined above.
    
    """
    
    mlb_games_df = get_mlb_data(start_date=start_date, end_date=end_date)

    mlb_games_df_clean = clean_mlb_data(mlb_games_df)

    venue_coordinates = get_venue_coordinates()
    mlb_games_with_coordinates = mlb_games_df_clean.merge(
        venue_coordinates,
        how='left',
        left_on='home_team_name',
        right_on='team',
    ).drop('team', axis=1)

    weather_df = get_weather_data(mlb_games_with_coordinates)
    mlb_games_with_weather_data = mlb_games_with_coordinates.merge(
        weather_df,
        how='left',
        on=['time_zero_minute', 'lat', 'lng'],
    )

    mlb_games_with_weather_data_clean = drop_pii_and_extra_fields(mlb_games_with_weather_data)

    mlb_games_with_rand_id = add_random_id(mlb_games_with_weather_data_clean)

    mlb_games_with_jenny = add_jenny(mlb_games_with_rand_id)
    
    os.makedirs('data_backups', exist_ok=True)
    mlb_games_with_jenny.to_csv('data_backups/mlb_games.csv')
    mlb_games_with_jenny.to_parquet('data_backups/mlb_games.parquet')
    mlb_games_with_jenny.to_feather('data_backups/mlb_games.feather')
    
    upload_to_s3('data_backups/mlb_games.csv', 'dump/mlb_games.csv')
    upload_to_s3('data_backups/mlb_games.parquet', 'dump/mlb_games.parquet')
    upload_to_s3('data_backups/mlb_games.feather', 'dump/mlb_games.feather')
    print('successfully uploaded the transformed MLB games data to S3')


run_etl(start_date, end_date)