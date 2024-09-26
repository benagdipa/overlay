"""
#########################################################################
OL Optimisation Python Code
Developed by Ahmed Behiry for FW Engineering
FW Performance - September 2024 - Rel 10.0
#########################################################################
"""

import pandas as pd
import os
import sys
import functions as F
from all_process import main as all_main
from mode3_process import main as online_M3_main
from planet_process import main as planet_main
from file_process import main as offline_M3_main
from requests.auth import HTTPBasicAuth
import queries as Q
import trino
from trino.auth import BasicAuthentication
import warnings

# STARBURST connection setup
warnings.filterwarnings("ignore")
req_kwargs = {
        'auth': HTTPBasicAuth("SVC_THO_PRD_FW_PERF", "extent90@SUPPORT"),
        'verify': False
    }

conn = trino.dbapi.connect(
         host="dv.cdl.nbnco.net.au",
         port=443,
         http_scheme='https',
         auth=BasicAuthentication('SVC_THO_PRD_FW_PERF', 'extent90@SUPPORT'),
         verify=False,
         catalog='hive',
    )

# Read settings
min_dl_prb_th, min_ul_prb_th, rsrp_reduction_limit_1, rsrp_reduction_limit_2, good_rsrp, critical_rsrp, min_t_co_ch, min_s_co_ch, min_co_ch_gain, min_rsrp_gain_m3, min_rsrp_gain_pl, min_rsrp_gain_eq_co_ch = F.read_settings()
_ = os.system('cls')  # Clear console

# create output folders
path = os.getcwd()
queries_path = str(path) + fr"/queries"
output_path = str(path) + fr"/Outputs"
if not os.path.isdir('queries'):
    os.makedirs(queries_path)
if not os.path.isdir('Outputs'):
    os.makedirs(output_path)

dummy1 = F.print_notification("#########  Welcome to OL Automation Tool  #########")
dummy2 = F.print_header("Please select the required mode of operation from the below:")
mode_of_oper = input("1. Online Mode3 + Planet processing"+"\n"+"2. Online Mode3 processing"+"\n"+"3. Planet processing"+"\n"+"4. Clean up for new EPO"+"\n"+"provide your selection between 1 and 4: ")
while mode_of_oper not in ['1', '2', '3', '4']:
    mode_of_oper = input("provide your selection between 1 and 4: ")

_ = os.system('cls')  # Clear console
main_start = F.print_notification("######### Start processing #########")
if mode_of_oper == '1':
    concatenated_df, exceptions_dict, select = all_main()
elif mode_of_oper == '2':
    concatenated_df, exceptions_dict, select = online_M3_main()
elif mode_of_oper == '3':
    concatenated_df, exceptions_dict, select = planet_main()
elif mode_of_oper == '4':
    concatenated_df, exceptions_dict, select = offline_M3_main()
else:
    sys.exit()

# adding cc info
start = F.print_header("\nAdding number of DL and UL CC info....")
if select == '1':
    cc_qualified_df = pd.read_csv('queries/cc_query.csv')
else:
    try:
        cc_qualified_df = pd.read_sql(Q.cc_query, conn)
        cc_qualified_df.to_csv('queries/cc_query.csv', index=False)
    except:
        print("ERROR! no connection to THOR or error while running number of cc query")
        sys.exit()

concatenated_df = pd.merge(concatenated_df, cc_qualified_df[['s_cell', 's_dl_cc', 's_ul_cc']], on='s_cell', how='left').drop_duplicates(subset=['location_id','t_cell'])
cc_qualified_df = cc_qualified_df.rename(columns={'s_cell':'t_cell', 's_dl_cc': 't_dl_cc', 's_ul_cc': 't_ul_cc'})
concatenated_df = pd.merge(concatenated_df, cc_qualified_df[['t_cell', 't_dl_cc', 't_ul_cc']], on='t_cell', how='left').drop_duplicates(subset=['location_id','t_cell'])
F.print_done_header('Adding number of DL and UL CC info', start)

# export working file
start = F.print_header("\nExporting working file....")
F.print_done_header('Exporting working file', start)
concatenated_df.to_csv('Outputs/working file - all.csv', index=False)
concatenated_df = pd.read_csv('Outputs/working file - all.csv')
# loop over mode3 users to calculate source and target co channel deltas
if mode_of_oper in ['1', '2', '4']:
    start = F.print_header("\nCalculating Mode 3 Co Channel Delta....")
    loc_groups = concatenated_df.loc[concatenated_df['data_source'] == 'mode3'].groupby(by='location_id', axis=0)
    for loc_id, loc_group in loc_groups:
        for i in loc_group.index:
            if pd.notnull(loc_group.loc[i, 's_pci']) and pd.notnull(loc_group.loc[i, 's_earfcn']) and pd.notnull(loc_group.loc[i, 's_rsrp']):
                concatenated_df.loc[i, 's_co_channel_delta'] = F.find_co_channel(loc_group, loc_group.loc[i, 's_pci'], loc_group.loc[i, 's_earfcn'], loc_group.loc[i, 's_rsrp'], False)
            if pd.notnull(loc_group.loc[i, 't_pci']) and pd.notnull(loc_group.loc[i, 't_earfcn']) and pd.notnull(loc_group.loc[i, 't_rsrp']):
                concatenated_df.loc[i, 't_co_channel_delta'] = F.find_co_channel(loc_group, loc_group.loc[i, 't_pci'], loc_group.loc[i, 't_earfcn'], loc_group.loc[i, 't_rsrp'], True)
    F.print_done_header('Calculating Mode 3 Co Channel Delta', start)

# loop to calculate flags
start = F.print_header("\nCalculating flags....")
concatenated_df[['Co_Channel_flag', 'distance_flag', 'azimuth_flag', 'prb_flag', 'cc_flag', 'valid_move', 'Co_channel_gain', 'rsrp_gain']] = concatenated_df.apply(F.calculate_flags, axis=1, result_type='expand')
F.print_done_header('Calculating flags', start)

# loop to select the final move list
start = F.print_header("\nSelecting final moves list....")
final_moves = pd.DataFrame()
loc_groups = concatenated_df.loc[concatenated_df['valid_move'] == 1].groupby(by='location_id', axis=0)
for loc_id, loc_group in loc_groups:
    temp_df = pd.DataFrame()
    data_source, wntd_id, imsi, s_SiteName, s_cell, s_pci, t_SiteName, t_cell, t_pci, s_co_ch, t_co_ch, co_ch_gain, s_rsrp, t_rsrp, rsrp_gain, s_bearing, t_bearing = F.get_moves(loc_group)
    temp_df = pd.concat([temp_df, pd.DataFrame.from_records([
        {'data_source': data_source,
         'NTDID': wntd_id,
         'IMSI': imsi,
         's_SiteName': s_SiteName,
         's_cell': s_cell,
         'Current PCI': s_pci,
         't_SiteName': t_SiteName,
         't_cell': t_cell,
         'Target PCI': t_pci,
         's_co_channel_delta': s_co_ch,
         't_co_channel_delta': t_co_ch,
         'Co_channel_gain': co_ch_gain,
         's_rsrp': s_rsrp,
         't_rsrp': t_rsrp,
         'rsrp_gain': rsrp_gain,
         's_bearing': s_bearing,
         't_bearing': t_bearing}])])
    final_moves = pd.concat([final_moves, temp_df])

# inter and intra site flag
final_moves["Inter/Intra site move"] = final_moves.apply(lambda row: "intra site" if row['s_SiteName'] == row['t_SiteName'] else "inter site", axis=1)
final_moves["Inter/Intra beam move"] = final_moves.apply(lambda row: ("intra beam" if row['s_bearing'] == row['t_bearing'] else "inter beam") if row['s_SiteName'] == row['t_SiteName'] else None, axis=1)

# add ECI data
production_df = pd.read_csv('queries/production_query.csv')
final_moves = pd.merge(final_moves, production_df[['s_cell', 'Current ECI']], how='left', on='s_cell')
production_df = production_df.rename(columns={'s_cell': 't_cell', 'Current ECI': 'Target ECI'})
final_moves = pd.merge(final_moves, production_df[['t_cell', 'Target ECI']], how='left', on='t_cell')
final_moves = final_moves[['data_source', 'NTDID', 'IMSI', 's_SiteName', 's_cell', 'Current ECI', 'Current PCI', 't_SiteName', 't_cell', 'Target ECI', 'Target PCI', 's_co_channel_delta', 't_co_channel_delta', 'Co_channel_gain', 's_rsrp', 't_rsrp', 'rsrp_gain', 'Inter/Intra site move', 'Inter/Intra beam move']]

# High and low priority moves
mode3_moves_filter_1 = ((final_moves['data_source'] == 'mode3') & (final_moves['s_co_channel_delta'] < min_s_co_ch) & (final_moves['Co_channel_gain'] >= min_co_ch_gain))
mode3_moves_filter_2 = ((final_moves['data_source'] == 'mode3') & (final_moves['s_rsrp'] < -85) & (final_moves['rsrp_gain'] >= min_rsrp_gain_m3))
planet_moves_filter = ((final_moves['data_source'] == 'planet') & (final_moves['rsrp_gain'] >= min_rsrp_gain_pl))
high_prio_moves = final_moves[mode3_moves_filter_1 | mode3_moves_filter_2 | planet_moves_filter]
low_prio_moves = final_moves[~final_moves['NTDID'].isin(high_prio_moves['NTDID'].tolist())]

# calculate moves summary
in_high_prio_counts = high_prio_moves['t_cell'].value_counts().reset_index(name='in high prio moves').rename(columns={'t_cell': 'eutan cell ID'}).fillna(0)
in_low_prio_counts = low_prio_moves['t_cell'].value_counts().reset_index(name='in low prio moves').rename(columns={'t_cell': 'eutan cell ID'}).fillna(0)
out_high_prio_counts = high_prio_moves['s_cell'].value_counts().reset_index(name='out high prio moves').rename(columns={'s_cell': 'eutan cell ID'}).fillna(0)
out_low_prio_counts = low_prio_moves['s_cell'].value_counts().reset_index(name='out low prio moves').rename(columns={'s_cell': 'eutan cell ID'}).fillna(0)
user_count_df = pd.read_csv('queries/user_count_query.csv').rename(columns={'s_cell': 'eutan cell ID', 's_cell_name': 'cell name', 's_connected_devices': 'current user count'})
user_count_df = pd.merge(user_count_df, in_high_prio_counts, on='eutan cell ID', how='left')
user_count_df = pd.merge(user_count_df, in_low_prio_counts, on='eutan cell ID', how='left')
user_count_df = pd.merge(user_count_df, out_high_prio_counts, on='eutan cell ID', how='left')
user_count_df = pd.merge(user_count_df, out_low_prio_counts, on='eutan cell ID', how='left')
user_count_df = user_count_df.fillna(0)
user_count_df["user count post high priority moves only"] = user_count_df.apply(lambda row: row['current user count'] - row['out high prio moves'] + row['in high prio moves'], axis=1)
user_count_df["user count post all moves"] = user_count_df.apply(lambda row: row['current user count'] - (row['out low prio moves'] + row['out high prio moves']) + (row['in low prio moves'] + row['in high prio moves']), axis=1)
user_count_df = user_count_df[(user_count_df['in high prio moves'] != 0) | (user_count_df['in low prio moves'] != 0) | (user_count_df['out high prio moves'] != 0) | (user_count_df['out low prio moves'] != 0)]
user_count_df = user_count_df[['eutan cell ID', 'cell name', 'current user count', "user count post high priority moves only", "user count post all moves"]]
output_dict = {'High priority Moves': high_prio_moves, 'Low priority Moves': low_prio_moves, 'Cell user count summary': user_count_df}
F.print_done_header('Selecting final moves list', start)

# Export outputs
start = F.print_header("\nExporting output files....")
F.save_xls(output_dict, 'Outputs/Recommended moves.xlsx')
concatenated_df.sort_values(by=['wntd_id', 'data_source'], ascending=True).to_csv('Outputs/working file - all.csv', index=False)
concatenated_df.loc[concatenated_df['wntd_id'].isin(final_moves['NTDID'].tolist())].sort_values(by=['wntd_id', 'data_source'], ascending=True).to_csv('Outputs/working file - moves.csv', index=False)
F.save_xls(exceptions_dict, 'Outputs/summary.xlsx')
F.print_done_header('Exporting output files', start)

# print summary
start = F.print_header("\nSummary:")
if mode_of_oper in ['1', '2', '4']:
    print(f"{len(final_moves.loc[final_moves['data_source'] == 'mode3'])} - Mode 3 based moves proposed")
if mode_of_oper in ['1', '3']:
    print(f"{len(final_moves.loc[final_moves['data_source'] == 'planet'])} - Planet based moves proposed")
for key in exceptions_dict.keys():
    print(f"{len(exceptions_dict[key].drop_duplicates())} - {key}")

print("")
F.print_done_notification('\nOverall process', main_start)
