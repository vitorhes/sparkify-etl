import os
import glob
import pandas as pd

def get_files_path(filepath):
    all_files = []

    for dirpath, dirnames, filenames in os.walk(filepath):
        files = glob.glob(os.path.join(dirpath,"*.json"))

        for file in files:
            all_files.append(os.path.abspath(file))

    return all_files


def get_file_values (path,columns: list):
    file_paths = get_files_path(path)
    df_list = []

    for file_path in file_paths:

        df = pd.read_json(file_path, lines = True)
        df = df.drop_duplicates().dropna()
        df_ready = df[columns].values
   
        for stg in df_ready:
            df_list.append(stg)

    return df_list


def extract_datetime(path):
    file_paths = get_files_path(path)
    df_list = []

    for file_path in file_paths:

        df = pd.read_json(file_path, lines = True)
        df = df[df['page'] == 'NextSong']
        
        t = pd.to_datetime(df['ts'], unit = 'ms')
        time_data = [(tt.value, tt.hour, tt.day, tt.week, tt.month, tt.year, tt.weekday()) for tt in t]
        column_labels = ('timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday')
        time_df = pd.DataFrame(data = time_data, columns= column_labels)

        df_list.append(time_df)

    return df_list

def log_to_df(path):
    file_paths = get_files_path(path)
    df_list = []

    for file_path in file_paths:
        df = pd.read_json(file_path, lines = True)
        df = df.drop_duplicates().dropna()
        df_list.append(df)

    return df_list
