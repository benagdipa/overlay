import pandas as pd
import warnings
from requests.auth import HTTPBasicAuth
import queries as Q
import mode3_query as m3_q
import sys
import trino
from trino.auth import BasicAuthentication
import functions as F


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

    start = F.print_header("Running Thor queries....")
    try:
        start = F.print_activity("---Mode3 query running...")
        # mode3_df = pd.read_csv('queries/Mode3_Trial_area_20240213_Hue.csv')
        mode3_df = pd.read_sql(m3_q.Mod3_query, conn)
        mode3_df.to_csv('queries/mode3_query.csv', index=False)
        F.print_done_activity(f'---Mode3 query completed ({len(mode3_df)} Records generated)', start)

        start = F.print_activity("---Active AVC query running...")
        # active_avc_df = pd.read_csv('queries/active_avc_query.csv')
        active_avc_df = pd.read_sql(Q.active_avc_query, conn)
        active_avc_df.to_csv('queries/active_avc_query.csv', index=False)
        F.print_done_activity('---Active AVC query completed', start)

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

    except:
        print("ERROR! no connection to THOR or missing views")
        sys.exit()

    start = F.print_header("\nReading Planet NTP database....")
    try:
        # ntp_database_df = pd.read_csv('NTP Database Performance input - Copy.csv')
        ntp_database_df = pd.read_csv('NTP Database Performance input.csv')
    except:
        print("ERROR! NTP database missing on tool folder or wrong name")
        sys.exit()
    F.print_done_header('Reading Planet NTP database', start)

    output_dict = {}
    dummy1 = F.print_header("\nProcess Mode3 data....")
    start = F.print_activity("---Removing single record users...")
    # identify single record wntds
    mode3_locs = mode3_df[['location_id']]
    single_record_locs = mode3_locs[~mode3_locs.duplicated(keep=False)]
    output_dict['Mode3 single entry LOCs'] = single_record_locs

    # keep only wntds with more than one record
    multi_record_locs = mode3_locs[mode3_locs.duplicated(keep=False)]
    mode3_df = mode3_df.loc[mode3_df['location_id'].isin(multi_record_locs['location_id'].tolist())]
    F.print_done_activity('---Removing single record users', start)

    dummy2 = F.print_header("\nProcess Planet data and appending to Mode3....")
    start = F.print_activity("---Reading data from planet for users missing in mode 3 query output...")
    # identify WNTDs in Planet not in Mode 3
    mode3_locs = mode3_df[['location_id']]
    ntp_database_df = ntp_database_df.loc[~ntp_database_df['loc_id'].isin(mode3_locs['location_id'].tolist())]
    F.print_done_activity('---Reading data from planet for users missing in mode 3 query output', start)

    start = F.print_activity("---Filtering out users with 0 active AVC's...")
    # filter locs in NTP database with active avc only
    ntp_database_df = ntp_database_df.loc[ntp_database_df['loc_id'].isin(active_avc_df['location_id'].tolist())]
    F.print_done_activity("---Filtering out users with 0 active AVC's", start)

    # unpivot ntp database
    start = F.print_activity("---Unpivoting ntp database...")
    ntp_database_df_unpivoted = F.unpivot_df(ntp_database_df)
    F.print_done_activity("---Unpivoting ntp database", start)

    # merge eps data
    start = F.print_activity("---Merging EPS data...")
    ntp_database_df_unpivoted = ntp_database_df_unpivoted.drop('wntd_version', axis=1).rename(columns={'loc_id': 'location_id'})
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted, eps_df[['location_id', 'wntd_version', 'wntd_id', 'imsi', 's_cell', 's_site', 's_cell_name']], on='location_id',how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    F.print_done_activity("---Merging EPS data", start)

    # resolve target site name from target cell name
    ntp_database_df_unpivoted['t_site'] = ntp_database_df_unpivoted['t_cell_name'].str.split('_').str[0]

    # merge production data
    start = F.print_activity("---Merging pci/earfcn data...")
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted, production_df[['s_cell_name', 's_pci', 's_earfcn']],on='s_cell_name', how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    production_df = production_df.rename(columns={'s_cell_name': 't_cell_name', 's_cell': 't_cell', 's_pci': 't_pci', 's_earfcn': 't_earfcn'})
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted,production_df[['t_cell_name', 't_cell', 't_pci', 't_earfcn']],on='t_cell_name', how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    F.print_done_activity("---Merging pci/earfcn data", start)

    # merge conntected devices
    start = F.print_activity("---Merging count of users data...")
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted,user_count_df[['s_cell_name', 's_connected_devices']], on='s_cell_name',how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    user_count_df = user_count_df.rename(columns={'s_cell': 't_cell', 's_cell_name': 't_cell_name', 's_connected_devices': 't_connected_devices'})
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted,user_count_df[['t_cell_name', 't_connected_devices']], on='t_cell_name',how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    F.print_done_activity("---Merging count of users data", start)

    # merge appian data
    start = F.print_activity("---Merging appian data...")
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted, appian_df[['s_cell_name', 's_antenna_type', 's_cell_bearing', 's_cell_beamwidth', 's_lat', 's_long']], on='s_cell_name',how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    appian_df = appian_df.rename(columns={'s_cell_name': 't_cell_name', 's_antenna_type': 't_antenna_type', 's_cell_bearing': 't_cell_bearing','s_cell_beamwidth': 't_cell_beamwidth'})
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted, appian_df[['t_cell_name', 't_antenna_type', 't_cell_bearing', 't_cell_beamwidth']], on='t_cell_name',how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    F.print_done_activity("---Merging appian data", start)

    # merge prb_utlization bh
    start = F.print_activity("---Merging PRB utilization data...")
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted,bh_kpis_df[['s_cell_name', 's_prb_util_dl_r14', 's_prb_util_ul_r14']],on='s_cell_name', how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    bh_kpis_df = bh_kpis_df.rename(columns={'s_cell_name': 't_cell_name', 's_prb_util_dl_r14': 't_prb_util_dl_r14','s_prb_util_ul_r14': 't_prb_util_ul_r14'})
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted,bh_kpis_df[['t_cell_name', 't_prb_util_dl_r14', 't_prb_util_ul_r14']],on='t_cell_name', how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    F.print_done_activity("---Merging PRB utilization data", start)

    # resolve s_rsrp and s_cinr mapping
    start = F.print_activity("---Resolving s_rsrp and s_cinr values...")
    ntp_database_df_unpivoted['t_combo'] = ntp_database_df_unpivoted.apply(lambda row: row['location_id'] + '-' + str(row['t_pci']).replace('.0', '') + '-' + str(row['t_earfcn']).replace('.0', ''), axis=1)
    ntp_database_df_unpivoted['s_combo'] = ntp_database_df_unpivoted.apply(lambda row: row['location_id'] + '-' + str(row['s_pci']).replace('.0', '') + '-' + str(row['s_earfcn']).replace('.0', ''), axis=1)
    mapping = ntp_database_df_unpivoted[['t_combo', 't_rsrp', 't_cinr']]
    mapping = mapping.rename(columns={'t_combo': 's_combo', 't_rsrp': 's_rsrp', 't_cinr': 's_cinr'}).drop_duplicates(subset=['s_combo'])
    ntp_database_df_unpivoted = pd.merge(ntp_database_df_unpivoted, mapping[['s_combo', 's_rsrp', 's_cinr']],on='s_combo', how='left').drop_duplicates(subset=['location_id', 't_cell_name'])
    F.print_done_activity("---Resolving s_rsrp and s_cinr values", start)

    # run calculated fields function on all the df
    start = F.print_activity("---Processing calculated fields...")
    ntp_database_df_unpivoted[['s_cell_edge_low', 's_cell_edge_high', 't_cell_edge_low', 't_cell_edge_high', 's_S2U_Bearing','s_U2S_Distance_Km', 's2t_connected_devices_delta', 's2t_prb_util_dl_delta', 's2t_prb_util_ul_delta','s_cell_S2U_Bearing_confirmed', 't_cell_S2U_Bearing_confirmed','s2t_rsrp_delta']] = ntp_database_df_unpivoted.apply(F.calculated_fields, axis=1, result_type='expand')
    F.print_done_activity("---Processing calculated fields", start)

    start = F.print_activity("---Appending to Mode 3 data and prepare final frame...")
    # add extra rows
    ntp_database_df_unpivoted.loc[:, ["newest_m3_date", "s_rsrq", "t_rsrq", "s2t_rsrq_delta"]] = ''
    ntp_database_df_unpivoted.loc[:, ["Channel_Status"]] = 'clean_channel'

    # add source label
    ntp_database_df_unpivoted.loc[:, 'data_source'] = 'planet'
    mode3_df.loc[:, 'data_source'] = 'mode3'

    # generate summary exceptions
    output_dict['LOCs missing EPS data'] = ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['wntd_version'].isnull() | ntp_database_df_unpivoted['wntd_id'].isnull() |ntp_database_df_unpivoted['imsi'].isnull() | ntp_database_df_unpivoted['s_cell'].isnull() |ntp_database_df_unpivoted['s_site'].isnull() | ntp_database_df_unpivoted['s_cell_name'].isnull()]['location_id']
    output_dict['Cells missing prod data'] = pd.concat([ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['t_cell'].isnull() |ntp_database_df_unpivoted['t_pci'].isnull() |ntp_database_df_unpivoted['t_earfcn'].isnull()].rename({'t_cell_name': 'cell name'}, axis=1)['cell name'], ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['s_pci'].isnull() |ntp_database_df_unpivoted['s_earfcn'].isnull()].rename({'s_cell_name': 'cell name'}, axis=1)['cell name']])
    output_dict['Cells missing user count'] = pd.concat([ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['t_connected_devices'].isnull()].rename(columns={'t_cell_name': 'cell name'})['cell name'],ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['s_connected_devices'].isnull()].rename(columns={'s_cell_name': 'cell name'})['cell name']])
    output_dict['Cells missing appian data'] = pd.concat([ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['t_antenna_type'].isnull() |ntp_database_df_unpivoted['t_cell_bearing'].isnull() |ntp_database_df_unpivoted['t_cell_beamwidth'].isnull()].rename(columns={'t_cell_name': 'cell name'})['cell name'], ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['s_antenna_type'].isnull() |ntp_database_df_unpivoted['s_cell_bearing'].isnull() |ntp_database_df_unpivoted['s_cell_beamwidth'].isnull() |ntp_database_df_unpivoted['s_lat'].isnull() |ntp_database_df_unpivoted['s_long'].isnull()].rename(columns={'s_cell_name': 'cell name'})['cell name']])
    output_dict['Cells missing prb data'] = pd.concat([ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['t_prb_util_dl_r14'].isnull()].rename(columns={'t_cell_name': 'cell name'})['cell name'],ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['s_prb_util_dl_r14'].isnull()].rename(columns={'s_cell_name': 'cell name'})['cell name']])
    output_dict['LOCs not on best predicted planet cells'] = ntp_database_df_unpivoted.loc[ntp_database_df_unpivoted['s_rsrp'].isnull() | ntp_database_df_unpivoted['s_cinr'].isnull()]['location_id']

    # final df
    ntp_database_final_df = ntp_database_df_unpivoted[
        ['newest_m3_date', 'wntd_version', 'wntd_id', 'imsi', 'location_id', 's_site', 't_site', 's_cell', 't_cell',
         's_pci', 't_pci', 's_rsrp', 't_rsrp', 's_cinr', 't_cinr', 's_rsrq', 't_rsrq', 's_earfcn', 't_earfcn',
         's_connected_devices', 't_connected_devices', 's_prb_util_ul_r14', 't_prb_util_ul_r14', 's_prb_util_dl_r14',
         't_prb_util_dl_r14', 's_antenna_type', 't_antenna_type', 's_cell_bearing', 't_cell_bearing',
         's_cell_beamwidth', 't_cell_beamwidth', 's_cell_edge_low', 's_S2U_Bearing', 's_cell_edge_high',
         't_cell_edge_low', 't_S2U_Bearing', 't_cell_edge_high', 's_U2S_Distance_Km', 't_U2S_Distance_Km',
         's2t_rsrp_delta', 's2t_rsrq_delta', 's2t_connected_devices_delta', 's2t_prb_util_ul_delta',
         's2t_prb_util_dl_delta', 's_cell_S2U_Bearing_confirmed', 't_cell_S2U_Bearing_confirmed', 'Channel_Status',
         'data_source']].drop_duplicates(subset=['location_id', 't_cell'])
    concatenated_df = pd.concat([ntp_database_final_df, mode3_df])
    F.print_done_activity("---Appending to Mode 3 data and prepare final frame", start)
    return concatenated_df, output_dict, '2'

