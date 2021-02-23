import pandas as pd
import glob
import multiprocessing
from joblib import Parallel, delayed
import numpy as np


# opens the csv file & stores it in a variable called raw_data
# returns the full raw data in pandas DataFrame
def open_csv(path):
    raw_data = pd.read_csv(path)
    return raw_data


# create a pandas dataframe of a specific season before the season started
# returns a pandas dataframe with the year of the season and the teams involved with initialized stats
def create_initial_table(path, raw_data):
    # extracts the season's year
    season_number = path[path.index("/") + 1:path.index(".")]
    # reduce the information to the relevant columns
    all_cols = ['HomeTeam', 'AwayTeam', 'FTHG', 'FTAG', 'FTR', 'HS', 'AS', 'HST', 'AST', 'HC', 'AC']
    match_list = raw_data.filter(items=all_cols)
    ### checking if the filter function filtered anything - if not the  value is TRUE
    ### filt = (match_list.equals(raw_data[all_cols]))
    ### print(filt)
    # taking the values of the reduced data table and insert it to a list
    clubs_in_season = []
    # iterate over the complete data and inserts the clubs participated in the league that year
    for ind, fixture in match_list.iterrows():
        if fixture['HomeTeam'] not in clubs_in_season:
            clubs_in_season.append(fixture[0])
        elif fixture['AwayTeam'] not in clubs_in_season:
            clubs_in_season.append(fixture[1])
    # setting the columns for the DataFrame
    season_DF_columns = ['Pos', 'Club', 'Pld', 'W', 'D', 'L', 'GF', 'GA', 'GD', 'pts',
                                        'HW%', 'HD%', 'HL%', 'AW%', 'AD%', 'AL%']
    # if the filter did not filter anything then Corners & shots are counted. extend the dataframe column
    #if filt:
    season_DF_columns.extend(['Corners', 'HCorners', 'ACorners', 'Shots', 'HShots', 'AShots', 'ShotsTarget',
                              'HShotsTarget', 'AShotsTarget', 'ShotsTarget%', 'HShotsTarget%', 'AShotsTarget%'])
    # create a dataframe with the columns stated above where the club's name is the index.
    season_data = pd.DataFrame(columns=season_DF_columns,
                               index=clubs_in_season)
    # set all values in the DF to 0
    season_data.loc[:] = 0.0
    # set a property for the DF - Title - that contains the season's year
    season_data.Title = 'Season ' + str(season_number)
    return season_data, match_list


# fill table in case of H win
def fill_table_H(fixture, season_tbl):
    season_tbl.at[fixture['HomeTeam'], 'W'] += 1
    season_tbl.at[fixture['HomeTeam'], 'HW%'] += 1
    season_tbl.at[fixture['HomeTeam'], 'pts'] += 3
    season_tbl.at[fixture['AwayTeam'], 'L'] += 1
    season_tbl.at[fixture['AwayTeam'], 'AL%'] += 1
    return season_tbl


# fill table in case of D
def fill_table_D(fixture, season_tbl):
    season_tbl.at[fixture['HomeTeam'], 'D'] += 1
    season_tbl.at[fixture['HomeTeam'], 'HD%'] += 1
    season_tbl.at[fixture['HomeTeam'], 'pts'] += 1
    season_tbl.at[fixture['AwayTeam'], 'D'] += 1
    season_tbl.at[fixture['AwayTeam'], 'AD%'] += 1
    season_tbl.at[fixture['AwayTeam'], 'pts'] += 1
    return season_tbl


# fill table in case of A win
def fill_table_A(fixture, season_tbl):
    season_tbl.at[fixture['HomeTeam'], 'L'] += 1
    season_tbl.at[fixture['HomeTeam'], 'HL%'] += 1
    season_tbl.at[fixture['AwayTeam'], 'W'] += 1
    season_tbl.at[fixture['AwayTeam'], 'AW%'] += 1
    season_tbl.at[fixture['AwayTeam'], 'pts'] += 3
    return season_tbl


# fill table with stats that does not matter who wins
def fill_table_Anyway(fixture, season_tbl):
    season_tbl.at[fixture['HomeTeam'], 'Pld'] += 1
    season_tbl.at[fixture['HomeTeam'], 'GF'] += fixture['FTHG']
    season_tbl.at[fixture['HomeTeam'], 'GA'] += fixture['FTAG']
    season_tbl.at[fixture['HomeTeam'], 'GD'] = season_tbl.at[fixture['HomeTeam'], 'GF'] - season_tbl.at[fixture['HomeTeam'], 'GA']

    season_tbl.at[fixture['AwayTeam'], 'Pld'] += 1
    season_tbl.at[fixture['AwayTeam'], 'GF'] += fixture['FTAG']
    season_tbl.at[fixture['AwayTeam'], 'GA'] += fixture['FTHG']
    season_tbl.at[fixture['AwayTeam'], 'GD'] = season_tbl.at[fixture['AwayTeam'], 'GF'] - season_tbl.at[fixture['AwayTeam'], 'GA']
    try:
        season_tbl.at[fixture['HomeTeam'], 'Corners'] += fixture['HC']
        season_tbl.at[fixture['HomeTeam'], 'HCorners'] += fixture['HC']
        season_tbl.at[fixture['HomeTeam'], 'Shots'] += fixture['HS']
        season_tbl.at[fixture['HomeTeam'], 'HShots'] += fixture['HS']
        season_tbl.at[fixture['HomeTeam'], 'ShotsTarget'] += fixture['HST']
        season_tbl.at[fixture['HomeTeam'], 'HShotsTarget'] += fixture['HST']

        season_tbl.at[fixture['AwayTeam'], 'Corners'] += fixture['AC']
        season_tbl.at[fixture['AwayTeam'], 'ACorners'] += fixture['AC']
        season_tbl.at[fixture['AwayTeam'], 'Shots'] += fixture['AS']
        season_tbl.at[fixture['AwayTeam'], 'AShots'] += fixture['AS']
        season_tbl.at[fixture['AwayTeam'], 'ShotsTarget'] += fixture['AST']
        season_tbl.at[fixture['AwayTeam'], 'AShotsTarget'] += fixture['AST']
    except:
        pass
    return season_tbl


# fill table sort percentage, pos and club name
def fill_table_Percentage(season_tbl):
    for row in range(0, len(season_tbl.index)):
        season_tbl.iloc[row].at['Pos'] = row + 1
        season_tbl.iloc[row].at['Club'] = season_tbl.index[row]
        season_tbl.iloc[row].at['HW%'] /= 19
        season_tbl.iloc[row].at['HD%'] /= 19
        season_tbl.iloc[row].at['HL%'] /= 19
        season_tbl.iloc[row].at['AW%'] /= 19
        season_tbl.iloc[row].at['AD%'] /= 19
        season_tbl.iloc[row].at['AL%'] /= 19
        try:
            season_tbl.iloc[row].at['ShotsTarget%'] = season_tbl.iloc[row].at['ShotsTarget'] / season_tbl.iloc[row].at['Shots']
            season_tbl.iloc[row].at['HShotsTarget%'] = season_tbl.iloc[row].at['HShotsTarget'] / season_tbl.iloc[row].at['HShots']
            season_tbl.iloc[row].at['AShotsTarget%'] = season_tbl.iloc[row].at['AShotsTarget'] / season_tbl.iloc[row].at['AShots']
        except:
            pass
    return season_tbl


# fills the table according to the data from the matches
# returns the a dictionary with the table at the end of the season
def fill_table(match_list, season_tbl):
    for ind, fixture in match_list.iterrows():
        # updates the wins, lose, and pts columns in case of home win
        if fixture['FTR'] == 'H':
            season_tbl = fill_table_H(fixture, season_tbl)
        # updates the draw and pts columns in case of a draw
        elif fixture['FTR'] == 'D':
            season_tbl = fill_table_D(fixture, season_tbl)
        # updates the table in any other case which is strictly away win
        elif fixture['FTR'] == 'A':
            season_tbl = fill_table_A(fixture, season_tbl)
        # updates the table with goal status anyway
        season_tbl = fill_table_Anyway(fixture, season_tbl)
    # makes sure the table will be sorted correctly comes the end of the season - first sort by Points (pts), if points
    # are equal sort between them according to Goal Difference (GD), then if both are equal sort by Goals For (GF)
    season_tbl = season_tbl.sort_values(by=['pts', 'GD', 'GF'], ascending=[False, False, False])
    # converts the number of home & away W/D/L to percentage
    season_tbl = fill_table_Percentage(season_tbl)
    return season_tbl


# styling the table
def style_table(data):
    pass


# retrieves the full season table and saves it as a csv file
# a void function - doesn't return anything
def retrieve_full_season_table(path):
    match_data = open_csv(path)
    season, match_data = create_initial_table(path, match_data)
    title = season.Title
    season = fill_table(match_data, season)
    season.Title = title
    # saving csv files of the full season tables
    new_file_name = "Full Season/" + str(season.Title)[-5:] + ".csv"
    season.to_csv(new_file_name, na_rep=0, index=False, float_format='%g')
    pass


# Goes over the raw data and creates full history of the premier league (full tables of every season)
# a void function - doesn't return anything
def create_complete_history_csv():
    list_of_files = glob.glob('Game By Game Data/*.csv')
    # i = 1
    for file_path in list_of_files:
        # print(i)
        retrieve_full_season_table(file_path)
        # i += 1
    pass


# Creates a dictionary of all PL seasons with the number of season as a key and pandas dataframe as the value,
# and a dictionary of match list of each season with season number as key and pandas dataframe as value.
# returns the full PL seasons dictionary and seasons match list dictionary
def create_full_history_dict():
    list_of_full_seasons = glob.glob('Full Season/*.csv')
    list_of_season_matches = glob.glob('Game By Game Data/*.csv')
    dict_season = {}
    dict_matches = {}
    for file_path in list_of_full_seasons:
        season_number = file_path[file_path.index("/") + 1:file_path.index(".")]
        dict_season[season_number] = open_csv(file_path).fillna(0)
        dict_season[season_number] = dict_season[season_number].set_index('Pos')
    for file_path in list_of_season_matches:
        season_number = file_path[file_path.index("/") + 1:file_path.index(".")]
        dict_matches[season_number] = open_csv(file_path).fillna(0)
    return dict_season, dict_matches


# create empty dataframe and set all cells to zero
def find_stats_per_position_Create_empty_dataframe(full_dict):
    # defining the columns header of the dataframe, cause for some reason it cant be done directly in the dataframe line
    seasons_list = list(full_dict.keys())
    seasons_list.insert(0, 'Pos')
    seasons_list.extend(['mean', 'median', 'stdev', 'max upper dv', 'max lower dv'])
    # defining initial values for the data frame - running integer for position, and 0 for any other slot
    dataframe_initial = np.zeros((20, len(seasons_list)))
    dataframe_initial[:, 0] = np.array(range(1, 21))
    # creating an empty dataframe with positions as index and season years
    stats_by_position = pd.DataFrame(dataframe_initial, columns=seasons_list)
    stats_by_position = stats_by_position.set_index('Pos')
    return stats_by_position


# fill the DataFrame with the specified stat in the appropriate pos and season
def find_stats_per_position_Fill_stats_in_dataframe(stats_by_position, full_dict, stat):
    # dict.items() returns a list of tuples as follow: (key,val)
    for season in full_dict.items():
        for pos in range(1, 21):
            # writes in the empty DF in the appropriate row the stat at the season's cell
            stats_by_position.loc[pos].at[season[0]] = season[1].loc[pos].at[stat]
    return stats_by_position


# calculate the statistical data and sets the appropriate cells
def find_stats_per_position_Calc_and_set_statistical(stats_by_position):
    for pos in range(1, 21):
        stats_by_position.loc[pos].at['mean'] = np.mean(stats_by_position.values[pos-1][:-5])
        stats_by_position.loc[pos].at['median'] = np.median(stats_by_position.values[pos-1][:-5])
        stats_by_position.loc[pos].at['stdev'] = np.std(stats_by_position.values[pos-1][:-5])
        stats_by_position.loc[pos].at['max upper dv'] = abs(max(stats_by_position.values[pos-1][:-5] -
                                                                stats_by_position.loc[pos].at['mean']))
        stats_by_position.loc[pos].at['max lower dv'] = abs(min(stats_by_position.values[pos-1][:-5] -
                                                                stats_by_position.loc[pos].at['mean']))
    return stats_by_position


# makes an analysis of the history per position on a list of desired stats
def find_stats_per_position(full_dict, exclude939495, stat):
    stats_by_position = find_stats_per_position_Create_empty_dataframe(full_dict)
    # dropping the unwanted seasons
    if exclude939495:
        stats_by_position = stats_by_position.drop(['93-94', '94-95'], axis=1)

    ## checking the number of cores available on the computer
    ## num_of_cores = multiprocessing.cpu_count()
    ### should be doing parallel for, but cant find how to do it

    stats_by_position = find_stats_per_position_Fill_stats_in_dataframe(stats_by_position, full_dict, stat)
    stats_by_position = find_stats_per_position_Calc_and_set_statistical(stats_by_position)
    stats_by_position.to_csv('Stats Per Position/stats per position - ' + stat + '.csv', float_format='%g')
    pass


# creates a dictionary with position ranges as keys and
# a list of all the clubs that finished the season within this range
# returns this dictionary
def check_position_range(season):
    season_position_range_dict = {'Champions': [], '2-4 CL Qualified': [], '5-7 UEFA League Qualified': [],
                                  '8-13or15 Upper Mid-Table': [], '14or16-17or19 Low Mid-Table': [], 'Relegated': []}
    for index, row in season.iterrows():
        if index == 1:
            season_position_range_dict['Champions'].append(row.at['Club'])
        elif 2 <= index <= 4:
            season_position_range_dict['2-4 CL Qualified'].append(row.at['Club'])
        elif 5 <= index <= 7:
            season_position_range_dict['5-7 UEFA League Qualified'].append(row.at['Club'])
        elif 8 <= index <= len(season.index)-7:
            season_position_range_dict['8-13or15 Upper Mid-Table'].append(row.at['Club'])
        elif len(season.index)-6 <= index <= len(season.index)-3:
            season_position_range_dict['14or16-17or19 Low Mid-Table'].append(row.at['Club'])
        elif len(season.index)-2 <= index <= len(season.index):
            season_position_range_dict['Relegated'].append(row.at['Club'])
    return season_position_range_dict


# gets a dictionary and a value
# returns the key in which the value is contained
def get_dict_key(dictionary, val):
    for key, value in dictionary.items():
        if val in value:
            return key
    return "Value doesn't exist"


# adds the specified columns and sets their initial values to zero
def add_result_percentage_by_position_range_Add_columns_and_initialize(seasonDF, dict_position_range):
    temp_parameters_set = ['pts vs ', 'W vs ', 'D vs ', 'L vs ', 'HW% vs ', 'HD% vs ', 'HL% vs ', 'AW% vs ', 'AD% vs ',
                           'AL% vs ']
    # creating new columns and initializing their values to 0
    const_parameter_set = []
    for pos_range in list(dict_position_range.keys()):
        for par in temp_parameters_set:
            const_parameter_set.append(par + pos_range)
    for val in const_parameter_set:
        seasonDF[val] = 0.0
    return seasonDF


# Changes the values of columns in case of home win
def add_result_percentage_by_position_range_H(seasonDF, fixture, dict_position_range):
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['HomeTeam']].tolist()[0], 'pts vs ' + get_dict_key(dict_position_range, fixture['AwayTeam'])] += 3
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['HomeTeam']].tolist()[0], 'W vs ' + get_dict_key(dict_position_range, fixture['AwayTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['HomeTeam']].tolist()[0], 'HW% vs ' + get_dict_key(dict_position_range, fixture['AwayTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['AwayTeam']].tolist()[0], 'L vs ' + get_dict_key(dict_position_range, fixture['HomeTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['AwayTeam']].tolist()[0], 'AL% vs ' + get_dict_key(dict_position_range, fixture['HomeTeam'])] += 1
    return seasonDF


# Changes the values of columns in case of D
def add_result_percentage_by_position_range_D(seasonDF, fixture, dict_position_range):
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['HomeTeam']].tolist()[0], 'D vs ' + get_dict_key(dict_position_range, fixture['AwayTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['HomeTeam']].tolist()[0], 'HD% vs ' + get_dict_key(dict_position_range, fixture['AwayTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['HomeTeam']].tolist()[0], 'pts vs ' + get_dict_key(dict_position_range, fixture['AwayTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['AwayTeam']].tolist()[0], 'D vs ' + get_dict_key(dict_position_range, fixture['HomeTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['AwayTeam']].tolist()[0], 'AD% vs ' + get_dict_key(dict_position_range, fixture['HomeTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['AwayTeam']].tolist()[0], 'pts vs ' + get_dict_key(dict_position_range, fixture['HomeTeam'])] += 1
    return seasonDF


# Changes the values of columns in case of AW
def add_result_percentage_by_position_range_A(seasonDF, fixture, dict_position_range):
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['HomeTeam']].tolist()[0], 'L vs ' + get_dict_key(dict_position_range, fixture['AwayTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['HomeTeam']].tolist()[0], 'HL% vs ' + get_dict_key(dict_position_range, fixture['AwayTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['AwayTeam']].tolist()[0], 'pts vs ' + get_dict_key(dict_position_range, fixture['HomeTeam'])] += 3
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['AwayTeam']].tolist()[0], 'W vs ' + get_dict_key(dict_position_range, fixture['HomeTeam'])] += 1
    seasonDF.at[seasonDF.index[seasonDF['Club'] == fixture['AwayTeam']].tolist()[0], 'AW% vs ' + get_dict_key(dict_position_range, fixture['HomeTeam'])] += 1
    return seasonDF


# converts the necessary columns to percentage
def add_result_percentage_by_position_range_Calc_percentage(seasonDF, dict_position_range):
    stat_percentage_list = ['HW%', 'HD%', 'HL%', 'AW%', 'AD%', 'AL%']#, 'ShotsTarget%', 'HShotsTarget%', 'AShotsTarget%']
    # ['Shots vs ', 'ShotsTarget vs ',  'HShots vs ', 'HShotsTarget vs ', 'AShots vs ', 'AShotsTarget vs ']
    stats_for_individual_analysis = []
    for pos in range(1, len(seasonDF.index)+1):
        for key, values in dict_position_range.items():
            if pos == 1:
                stats_for_individual_analysis.append('pts vs ' + key)
            for stat in stat_percentage_list:
                if pos == 1:
                    stats_for_individual_analysis.append(stat + ' vs ' + key)
                seasonDF.at[pos, stat + ' vs ' + key] = float(seasonDF.at[pos, stat + ' vs ' + key])/len(values)
    return seasonDF, stats_for_individual_analysis


# makes an analysis per position regarding W,D,L results of other clubs with respect to their position
# coming end of the season.
# e.g. in season 18-19 the 16th place won 5 matches against a club that finished in the top 4
def add_result_percentage_by_position_range(season_dict, matches_dict):
    for season in list(season_dict.keys()):
        # retrieving the position's dictionary
        dict_position_range = check_position_range(season_dict[season])
        # add column list and initialize values
        season_dict[season] = add_result_percentage_by_position_range_Add_columns_and_initialize(season_dict[season], dict_position_range)
        for ind, fixture in matches_dict[season].iterrows():
            if fixture['FTR'] == 'H':
                season_dict[season] = add_result_percentage_by_position_range_H(season_dict[season], fixture, dict_position_range)
            elif fixture['FTR'] == 'D':
                season_dict[season] = add_result_percentage_by_position_range_D(season_dict[season], fixture, dict_position_range)
            elif fixture['FTR'] == 'A':
                season_dict[season] = add_result_percentage_by_position_range_A(season_dict[season], fixture, dict_position_range)
        # turns the necessary values to percentage
        season_dict[season], stats_list = add_result_percentage_by_position_range_Calc_percentage(season_dict[season], dict_position_range)
        season_dict[season].to_csv('Full Season/' + season + '.csv', float_format='%g')
    return stats_list


# checks for different correlations in stats within a given season and per position
# def check_stat_correlation(season_dict, match_dict, stat):



def main():
    # makes sure the creation of the tables are not being done without necessity
    if not glob.glob('Full Season/*'):
        create_complete_history_csv()
    ### problem - the headers row features the word 'unnamed:', then the value 0 over the club's name

    # specifying the stat desired to analyze per position, and weather to exclude seasons with 22 teams (True)
    stat_list = [('pts', True), ('HW%', True), ('HD%', True), ('HL%', True), ('AW%', True), ('AD%', True),
                 ('AL%', True), ('Shots', True), ('ShotsTarget', True), ('ShotsTarget%', True),
                 ('HShots', True), ('HShotsTarget', True), ('HShotsTarget%', True), ('AShots', True),
                 ('AShotsTarget', True), ('AShotsTarget%', True)]

    full_history_dict, full_matches_dict = create_full_history_dict()
    if 'pts vs Champions' not in list(full_history_dict.values())[0].columns:
        stat_list_for_position_analysis = add_result_percentage_by_position_range(full_history_dict, full_matches_dict)
        # adding the stats from the position range analysis
        for stat in stat_list_for_position_analysis:
            stat_list.append((stat, True))

    for stat in stat_list:
        # makes sure the creation of the tables are not being done without necessity
        if not glob.glob('Stats Per Position/stats per position - ' + stat[0] + '.csv'):
            find_stats_per_position(full_history_dict, stat[1], stat[0])

    # full_history_dict, full_matches_dict = create_full_history_dict()

    print('Done!')


if __name__ == "__main__":
    main()


