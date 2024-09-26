import pandas as pd
import warnings
from requests.auth import HTTPBasicAuth
import queries as Q
import sys
import trino
from trino.auth import BasicAuthentication
import functions as F
from tkinter import filedialog
import h3

def main():
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

    input("Please browse Mode 3 file" + "\n" + "Press enter to open browser")
    inputfile = filedialog.askopenfilename(title="Please browse Mode 3 file", filetypes=[("MSExcel file", "*.xlsx")])
    mode3_df = pd.read_excel(inputfile).rename(columns={'IMSI': 'imsi', 'PCI': 't_pci', 'RSRP': 't_rsrp', 'CINR': 't_cinr', 'RSRQ': 't_rsrq', 'EARFCN': 't_earfcn'})
    select = input("\n" + "1. Use Cached queries" + "\n" + "2. run new queries" + "\n" + "provide your selection 1 or 2: ")
    while select not in ['1', '2']:
        select = input("provide your selection 1 or 2: ")
    if select == '1':
        start = F.print_header("Reading Cached queries....")
        eps_df = pd.read_csv('queries/eps_query.csv')
        user_count_df = pd.read_csv('queries/user_count_query.csv')
        production_df = pd.read_csv('queries/production_query.csv')
        appian_df = pd.read_csv('queries/appian_query.csv')
        bh_kpis_df = pd.read_csv('queries/bh_kpis_query.csv')
        coordinates = pd.read_csv('queries/coordinates.csv').dropna(subset=['Code'])
        F.print_done_header('Reading Cached queries', start)
    elif select == '2':
        start = F.print_header("Running queries....")
        try:
            start = F.print_activity("---EPS query running...")
            # eps_df = pd.read_csv('queries/eps_query.csv')
            eps_df = pd.read_sql(Q.eps_query, conn)
            eps_df.to_csv('queries/eps_query.csv', index=False)
            F.print_done_activity('---EPS query completed', start)

            start = F.print_activity("---Users count query running...")
            # user_count_df = pd.read_csv('queries/user_count_query.csv')
            user_count_df = pd.read_sql(Q.user_count_query, conn)
            user_count_df.to_csv('queries/user_count_query.csv', index=False)
            F.print_done_activity('---Users count query completed', start)

            start = F.print_activity("---Production data query running...")
            # production_df = pd.read_csv('queries/production_query.csv')
            production_df = pd.read_sql(Q.production_query, conn)
            production_df.to_csv('queries/production_query.csv', index=False)
            F.print_done_activity('---Production data query completed', start)

            start = F.print_activity("---Appian data query running...")
            # appian_df = pd.read_csv('queries/appian_query.csv')
            appian_df = pd.read_sql(Q.appian_query, conn)
            appian_df.to_csv('queries/appian_query.csv', index=False)
            F.print_done_activity('---Appian data query completed', start)

            start = F.print_activity("---BH KPIs query running...")
            # bh_kpis_df = pd.read_csv('queries/bh_kpis_query.csv')
            bh_kpis_df = pd.read_sql(Q.bh_kpis_query, conn)
            bh_kpis_df.to_csv('queries/bh_kpis_query.csv', index=False)
            F.print_done_activity('---BH KPIs query completed', start)

            start = F.print_activity("---Sites coordinates query running...")
            # coordinates = pd.read_csv('queries/coordinates.csv')
            coordinates = pd.read_sql(Q.coordinates_query, conn).dropna(subset=['Code'])
            coordinates.to_csv('queries/coordinates.csv', index=False)
            F.print_done_activity('---Sites coordinates query running', start)
        except:
            print("ERROR! no connection to THOR or missing views")
            sys.exit()
    else:
        sys.exit()

    dummy1 = F.print_header("\nProcess Mode3 file....")
    start = F.print_activity("---Resolving scanned PCI and EARFCN cell and site names")
    mode3_df['imsi'] = pd.to_numeric(mode3_df['imsi'], errors='coerce')  # convert imsi to numeric in Mode 3 frame for joining
    eps_df['imsi'] = pd.to_numeric(eps_df['imsi'], errors='coerce')  # convert imsi to numeric in EPS frame for joining
    mode3_df = pd.merge(mode3_df, eps_df, how='left', on='imsi')  # join to get serving site and cell data
    # mode3_df.to_csv('mode3_df.csv', index=False)
    no_eps_site = mode3_df[mode3_df['EPS site code'].isnull()]['imsi'].drop_duplicates()
    mode3_df = mode3_df.dropna(subset=['EPS site code'])
    mode3_df['code_earfcn_pci'] = mode3_df.apply(lambda row: str(row['EPS site code']) + "_" + str(row['t_earfcn']).replace('.0', '') + "_" + str(row['t_pci']).replace('.0', ''), axis=1)  # create joining identifier
    # Query Scanned EARFCN PCI pair data from THOR
    ENM = pd.DataFrame()
    for site_code in mode3_df['EPS site code'].unique().tolist():
        # print(site_code)
        try:
            temp = pd.read_sql(Q.ENM_query_part1 + F.get_site_codes(site_code, coordinates, 40) + Q.ENM_query_part2, conn)
        except:
            print("ERROR! no connection to THOR or missing views")
            sys.exit()
        temp.insert(0, 'site_code', site_code)
        temp['code_earfcn_pci'] = temp['site_code'].astype(str) + "_" + temp['earfcn'].astype(str) + "_" + temp['pci'].astype(str)
        ENM = pd.concat([ENM, temp])
    mode3_df = pd.merge(mode3_df, ENM[['code_earfcn_pci', 't_cell', 't_site', 'Scanned Site Code']],on='code_earfcn_pci', how='left')
    mode3_df = pd.merge(mode3_df, coordinates, left_on='Scanned Site Code', right_on='Code')
    mode3_df['distance'] = mode3_df.apply(lambda row: h3.point_dist((row['actual_latitude'], row['actual_longitude']), (row['loc_lat'], row['loc_long'])),axis=1)
    mode3_df = mode3_df.sort_values(by=['wntd_id', 'distance'], ascending=True).drop_duplicates(subset=['wntd_id', 't_pci', 't_earfcn'], keep='first').sort_values(by=['wntd_id', 't_earfcn'], ascending=True)
    mode3_df = mode3_df[['imsi', 'wntd_id', 'wntd_version', 's_site', 's_cell', 't_site', 't_cell', 't_pci', 't_rsrp', 't_cinr', 't_rsrq', 't_earfcn', 'location_id', 'loc_lat', 'loc_long']]
    mode3_df['s_cell_name'] = mode3_df.apply(lambda row: (str(row['s_site']) + "_" + str(int(str(row['s_cell'])[-2:])) if str(row['s_cell'])[17] == '0' else str(row['s_site']) + "_" + str(int(str(row['s_cell'])[-2:]) + 100)) if pd.notnull(row['s_cell']) and pd.notnull(row['s_site']) else None, axis=1)
    mode3_df['t_cell_name'] = mode3_df.apply(lambda row: (str(row['t_site']) + "_" + str(int(str(row['t_cell'])[-2:])) if str(row['t_cell'])[17] == '0' else str(row['t_site']) + "_" + str(int(str(row['t_cell'])[-2:]) + 100)) if pd.notnull(row['t_cell']) and pd.notnull(row['t_site']) else None, axis=1)
    F.print_done_activity("---Resolving scanned PCI and EARFCN cell and site names", start)

    # merge production data
    start = F.print_activity("---Merging pci/earfcn data")
    mode3_df = pd.merge(mode3_df, production_df[['s_cell_name', 's_pci', 's_earfcn']],on='s_cell_name', how='left').drop_duplicates(subset=['location_id', 't_pci', 't_earfcn'])
    F.print_done_activity("---Merging pci/earfcn data", start)

    # merge conntected devices
    start = F.print_activity("---Merging count of users data")
    mode3_df = pd.merge(mode3_df, user_count_df[['s_cell_name', 's_connected_devices']], on='s_cell_name',how='left').drop_duplicates(subset=['location_id', 't_pci', 't_earfcn'])
    user_count_df = user_count_df.rename(columns={'s_cell': 't_cell', 's_cell_name': 't_cell_name', 's_connected_devices': 't_connected_devices'})
    mode3_df = pd.merge(mode3_df, user_count_df[['t_cell_name', 't_connected_devices']], on='t_cell_name',how='left').drop_duplicates(subset=['location_id', 't_pci', 't_earfcn'])
    F.print_done_activity("---Merging count of users data", start)

    # merge appian data
    start = F.print_activity("---Merging appian data")
    mode3_df = pd.merge(mode3_df, appian_df[['s_cell_name', 's_antenna_type', 's_cell_bearing', 's_cell_beamwidth', 's_lat', 's_long']], on='s_cell_name',how='left').drop_duplicates(subset=['location_id', 't_pci', 't_earfcn'])
    appian_df = appian_df.rename(columns={'s_cell_name': 't_cell_name', 's_antenna_type': 't_antenna_type', 's_cell_bearing': 't_cell_bearing','s_cell_beamwidth': 't_cell_beamwidth','s_lat': 't_lat','s_long': 't_long'})
    mode3_df = pd.merge(mode3_df, appian_df[['t_cell_name', 't_antenna_type', 't_cell_bearing', 't_cell_beamwidth', 't_lat', 't_long']], on='t_cell_name',how='left').drop_duplicates(subset=['location_id', 't_pci', 't_earfcn'])
    F.print_done_activity("---Merging appian data", start)

    # merge prb_utlization bh
    start = F.print_activity("---Merging PRB utilization data")
    mode3_df = pd.merge(mode3_df, bh_kpis_df[['s_cell_name', 's_prb_util_dl_r14', 's_prb_util_ul_r14']],on='s_cell_name', how='left').drop_duplicates(subset=['location_id', 't_pci', 't_earfcn'])
    bh_kpis_df = bh_kpis_df.rename(columns={'s_cell_name': 't_cell_name', 's_prb_util_dl_r14': 't_prb_util_dl_r14','s_prb_util_ul_r14': 't_prb_util_ul_r14'})
    mode3_df = pd.merge(mode3_df,bh_kpis_df[['t_cell_name', 't_prb_util_dl_r14', 't_prb_util_ul_r14']], on='t_cell_name', how='left').drop_duplicates(subset=['location_id', 't_pci', 't_earfcn'])
    F.print_done_activity("---Merging PRB utilization data", start)

    # resolve s_rsrp and s_cinr mapping
    start = F.print_activity("---Resolving s_rsrp, s_rsrq and s_cinr values")
    mode3_df['t_combo'] = mode3_df.apply(lambda row: row['location_id'] + '-' + str(row['t_pci']).replace('.0', '') + '-' + str(row['t_earfcn']).replace('.0', ''), axis=1)
    mode3_df['s_combo'] = mode3_df.apply(lambda row: row['location_id'] + '-' + str(row['s_pci']).replace('.0', '') + '-' + str(row['s_earfcn']).replace('.0', ''), axis=1)
    mapping = mode3_df[['t_combo', 't_rsrp', 't_cinr', 't_rsrq']]
    mapping = mapping.rename(columns={'t_combo': 's_combo', 't_rsrp': 's_rsrp', 't_cinr': 's_cinr', 't_rsrq': 's_rsrq'}).drop_duplicates(subset=['s_combo'])
    mode3_df = pd.merge(mode3_df, mapping[['s_combo', 's_rsrp', 's_cinr', 's_rsrq']],on='s_combo', how='left').drop_duplicates(subset=['location_id', 't_pci', 't_earfcn'])
    F.print_done_activity("---Resolving s_rsrp, s_rsrq and s_cinr values", start)

    # run calculated fields function on all the df
    start = F.print_activity("---Processing calculated fields")
    mode3_df['s_U2S_Distance_Km'] = mode3_df.apply(lambda row: h3.point_dist((float(row['s_lat']), float(row['s_long'])), (float(row['loc_lat']), float(row['loc_long']))),axis=1)
    mode3_df['t_U2S_Distance_Km'] = mode3_df.apply(lambda row: h3.point_dist((float(row['t_lat']), float(row['t_long'])), (float(row['loc_lat']), float(row['loc_long']))), axis=1)
    mode3_df['s_S2U_Bearing'] = mode3_df.apply(lambda row: F.calculate_bearing(float(row.loc['s_lat']), float(row.loc['s_long']),float(row.loc['loc_lat']), float(row.loc['loc_long'])), axis=1)
    mode3_df['t_S2U_Bearing'] = mode3_df.apply(lambda row: F.calculate_bearing(float(row.loc['t_lat']), float(row.loc['t_long']), float(row.loc['loc_lat']),float(row.loc['loc_long'])), axis=1)
    mode3_df['s_cell_edge_low'] = mode3_df.apply(lambda row: float(row['s_cell_bearing']) - float(row['s_cell_beamwidth']) / 2 + 360 if float(row['s_cell_bearing']) - float(row['s_cell_beamwidth']) / 2 < 0 else float(row['s_cell_bearing']) - float(row['s_cell_beamwidth']) / 2, axis=1)
    mode3_df['s_cell_edge_high'] = mode3_df.apply(lambda row: float(row['s_cell_bearing']) + float(row['s_cell_beamwidth']) / 2 - 360 if float(row['s_cell_bearing']) + float(row['s_cell_beamwidth']) / 2 > 359 else float(row['s_cell_bearing']) + float(row['s_cell_beamwidth']) / 2, axis=1)
    mode3_df['t_cell_edge_low'] = mode3_df.apply(lambda row: float(row['t_cell_bearing']) - float(row['t_cell_beamwidth']) / 2 + 360 if float(row['t_cell_bearing']) - float(row['t_cell_beamwidth']) / 2 < 0 else float(row['t_cell_bearing']) - float(row['t_cell_beamwidth']) / 2, axis=1)
    mode3_df['t_cell_edge_high'] = mode3_df.apply(lambda row: float(row['t_cell_bearing']) + float(row['t_cell_beamwidth']) / 2 - 360 if float(row['t_cell_bearing']) + float(row['t_cell_beamwidth']) / 2 > 359 else float(row['t_cell_bearing']) + float(row['t_cell_beamwidth']) / 2, axis=1)
    mode3_df['s_cell_S2U_Bearing_confirmed'] = mode3_df.apply(lambda row:
                                                                  ((1 if row['s_S2U_Bearing'] <= row['s_cell_edge_high'] and row['s_S2U_Bearing'] < 180
                                                                  else
                                                                  (1 if row['s_S2U_Bearing'] >= row['s_cell_edge_low'] and row['s_S2U_Bearing'] > 180 else 0))
                                                                  if row['s_cell_edge_high'] < row['s_cell_edge_low']
                                                                  else
                                                                  (1 if row['s_S2U_Bearing']>=row['s_cell_edge_low'] and row['s_S2U_Bearing']<=row['s_cell_edge_high'] else 0))
                                                                  if pd.notnull(row['s_S2U_Bearing']) and pd.notnull(row['s_cell_edge_low']) and pd.notnull(row['s_cell_edge_high'])
                                                                  else 0
                                                                    , axis=1)
    mode3_df['t_cell_S2U_Bearing_confirmed'] = mode3_df.apply(lambda row:
                                                                  ((1 if row['t_S2U_Bearing'] <= row['t_cell_edge_high'] and row['t_S2U_Bearing'] < 180
                                                                    else
                                                                    (1 if row['t_S2U_Bearing'] >= row['t_cell_edge_low'] and row['t_S2U_Bearing'] > 180 else 0))
                                                                   if row['t_cell_edge_high'] < row['t_cell_edge_low']
                                                                   else
                                                                   (1 if row['t_S2U_Bearing'] >= row['t_cell_edge_low'] and row['t_S2U_Bearing'] <=row['t_cell_edge_high'] else 0))
                                                                  if pd.notnull(row['t_S2U_Bearing']) and pd.notnull(row['t_cell_edge_low']) and pd.notnull(row['t_cell_edge_high'])
                                                                  else 0
                                                                    , axis=1)
    F.print_done_activity("---Processing calculated fields", start)

    start = F.print_activity("---Removing single record users")
    # identify single record wntds
    mode3_locs = mode3_df[['location_id']]
    output_dict = {'Mode3 single entry LOCs': mode3_locs[~mode3_locs.duplicated(keep=False)], 'Users with no EPS cell': no_eps_site}

    # keep only wntds with more than one record
    multi_record_locs = mode3_locs[mode3_locs.duplicated(keep=False)]
    mode3_df = mode3_df.loc[(mode3_df['location_id'].isin(multi_record_locs['location_id'].tolist())) & (mode3_df['s_cell_name'] != mode3_df['t_cell_name'])]
    F.print_done_activity('---Removing single record users', start)

    start = F.print_activity("---Prepare final frame")

    # add source label
    mode3_df.loc[:, 'data_source'] = 'mode3'

    # final df
    mode3_df = mode3_df[
        ['wntd_version', 'wntd_id', 'imsi', 'location_id', 's_site', 't_site', 's_cell', 't_cell',
         's_pci', 't_pci', 's_rsrp', 't_rsrp', 's_cinr', 't_cinr', 's_rsrq', 't_rsrq', 's_earfcn', 't_earfcn',
         's_connected_devices', 't_connected_devices', 's_prb_util_ul_r14', 't_prb_util_ul_r14', 's_prb_util_dl_r14',
         't_prb_util_dl_r14', 's_antenna_type', 't_antenna_type', 's_cell_bearing', 't_cell_bearing',
         's_cell_beamwidth', 't_cell_beamwidth', 's_cell_edge_low', 's_S2U_Bearing', 's_cell_edge_high',
         't_cell_edge_low', 't_S2U_Bearing', 't_cell_edge_high', 's_U2S_Distance_Km', 't_U2S_Distance_Km',
         's_cell_S2U_Bearing_confirmed', 't_cell_S2U_Bearing_confirmed', 'data_source']]
    mode3_df.to_csv('Outputs/processed_mode3.csv', index=False)
    F.print_done_activity("---Prepare final frame", start)

    return mode3_df, output_dict, select

# df1, df2 = main()
# df1.to_csv('converted_mode3.csv', index=False)
# F.save_xls(df2, 'exceptions.xlsx')