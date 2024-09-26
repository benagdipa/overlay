import pandas as pd
import h3
import math
import datetime
import sys
from pandas import ExcelWriter

class Format:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    DELETE = "\033[F"

def print_header(string):
    print(Format.OKBLUE + string + Format.ENDC)
    return datetime.datetime.now()

def print_activity(string):
    print(string)
    return datetime.datetime.now()

def print_notification(string):
    print(Format.OKCYAN + string + Format.ENDC)
    return datetime.datetime.now()

def print_done_header(string , start):
    sys.stdout.write(Format.DELETE)
    end = datetime.datetime.now()
    print(f"{Format.OKBLUE}{string}{Format.OKGREEN} (done in {end - start}){Format.ENDC}")

def print_done_activity(string , start):
    sys.stdout.write(Format.DELETE)
    end = datetime.datetime.now()
    print(f"{string}{Format.OKGREEN} (done in {end - start}){Format.ENDC}")

def print_done_notification(string , start):
    sys.stdout.write(Format.DELETE)
    end = datetime.datetime.now()
    print(f"{Format.OKCYAN}{string} (done in {end - start}){Format.ENDC}")


def calculate_bearing(Lat1, Long1, Lat2, Long2):
 lat1 = math.radians(Lat1)
 lat2 = math.radians(Lat2)
 diffLong = math.radians(Long2 - Long1)
 x = math.sin(diffLong) * math.cos(lat2)
 y = math.cos(lat1) * math.sin(lat2) - (math.sin(lat1) * math.cos(lat2) * math.cos(diffLong))
 initial_bearing = math.atan2(x, y)
 initial_bearing = math.degrees(initial_bearing)
 compass_bearing = (initial_bearing + 360) % 360
 return compass_bearing


def unpivot_df(df):
    df_unpivoted = pd.DataFrame()
    for freq in ['F1', 'F2', 'F3', 'F4', 'F5', 'Spect1', 'Spect2', 'Spect3', 'Spect4', 'Wa1', 'Wa10a', 'Wa11a', 'Wa12a','Wa13a', 'Wa2', 'Wa3', 'Wa4', 'Wa5', 'Wa6', 'Wa7', 'Wa8', 'Wa9a']:
        temp_df = df[
                ['loc_id', 'planet_lat', 'planet_long', 'lte_cell_name', 'lte_rsrp', 'lte_cinr', 'lte_max_dl_thp',
                 'lte-dl_cc_count', 'lte-max_ul_thp', 'lte-ul_cc_count', 'mmWave_at_loc_id', 'nr_cell_name', 'nr_los',
                 'nr_distance', 'nr-fr_type', 'nr_rsrp', 'nr_cinr', 'nr_max_dl_thp', 'nr-dl_cc_count', 'nr-max_ul_thp',
                 'nr-ul_cc_count', 'service_class', 'wntd_version', 'lte cell source', 'B42_Primary_Spectrum_Zone',
                 'Optimization_flag', 'Target_Cell_for_repan', 'predicted_best_cell_name_' + freq,
                 'predicted_best_cell_rsrp_' + freq, 'predicted_best_cell_sinr_' + freq, 'mase_pow_' + freq,
                 'predicted_best_cell_site_lat_' + freq, 'predicted_best_cell_site_long_' + freq,
                 'loc_id_to_cell_bearing_' + freq, 'cell_to_loc_id_bearing_' + freq, 'loc_id_to_cell_distance_' + freq,
                 'predicted_best_cell_DL_CC_' + freq, 'predicted_best_cell_UL_CC_' + freq]]
        temp_df = temp_df.rename(
                columns={'predicted_best_cell_name_' + freq: 't_cell_name',
                         'predicted_best_cell_rsrp_' + freq: 't_rsrp',
                         'predicted_best_cell_sinr_' + freq: 't_cinr',
                         'mase_pow_' + freq: 'mase_pow',
                         'predicted_best_cell_site_lat_' + freq: 't_lat',
                         'predicted_best_cell_site_long_' + freq: 't_long',
                         'loc_id_to_cell_bearing_' + freq: 'loc_id_to_cell_bearing',
                         'cell_to_loc_id_bearing_' + freq: 't_S2U_Bearing',
                         'loc_id_to_cell_distance_' + freq: 't_U2S_Distance_Km',
                         'predicted_best_cell_DL_CC_' + freq: 'predicted_best_cell_DL_CC',
                         'predicted_best_cell_UL_CC_' + freq: 'predicted_best_cell_UL_CC'})
        df_unpivoted = pd.concat([df_unpivoted, temp_df])
    df_unpivoted = df_unpivoted[df_unpivoted.t_cell_name.notnull()]
    return df_unpivoted

def calculated_fields(row):
    if pd.isnull(row['s_cell_bearing']) or pd.isnull(row['s_cell_beamwidth']) or row['s_cell_beamwidth'] == '' or row['s_cell_bearing'] == '':
        s_cell_edge_low = None
        s_cell_edge_high = None
    else:
        if float(row['s_cell_bearing']) - float(row['s_cell_beamwidth']) / 2 < 0:
            s_cell_edge_low = float(row['s_cell_bearing']) - float(row['s_cell_beamwidth']) / 2 + 360
        else:
            s_cell_edge_low = float(row['s_cell_bearing']) - float(row['s_cell_beamwidth']) / 2
        if float(row['s_cell_bearing']) + float(row['s_cell_beamwidth']) / 2 > 359:
            s_cell_edge_high = float(row['s_cell_bearing']) + float(row['s_cell_beamwidth']) / 2 - 360
        else:
            s_cell_edge_high = float(row['s_cell_bearing']) + float(row['s_cell_beamwidth']) / 2

    if pd.isnull(row['t_cell_bearing']) or pd.isnull(row['t_cell_beamwidth']) or row['t_cell_beamwidth'] == '' or row['t_cell_bearing'] == '':
        t_cell_edge_low = None
        t_cell_edge_high = None
    else:
        if float(row['t_cell_bearing']) - float(row['t_cell_beamwidth']) / 2 < 0:
            t_cell_edge_low = float(row['t_cell_bearing']) - float(row['t_cell_beamwidth']) / 2 + 360
        else:
            t_cell_edge_low = float(row['t_cell_bearing']) - float(row['t_cell_beamwidth']) / 2
        if float(row['t_cell_bearing']) + float(row['t_cell_beamwidth']) / 2 > 359:
            t_cell_edge_high = float(row['t_cell_bearing']) + float(row['t_cell_beamwidth']) / 2 - 360
        else:
            t_cell_edge_high = float(row['t_cell_bearing']) + float(row['t_cell_beamwidth']) / 2

    if pd.isnull(row['s_lat']) or pd.isnull(row['s_long']) or pd.isnull(row['planet_lat']) or pd.isnull(row['planet_long']) or row['s_lat'] == '' or row['s_long'] == '' or ['planet_lat'] == '' or row['planet_long'] == '':
        s_S2U_Bearing = None
        s_U2S_Distance_Km = None
    else:
        s_S2U_Bearing = calculate_bearing(float(row.loc['s_lat']), float(row.loc['s_long']),float(row.loc['planet_lat']), float(row.loc['planet_long']))
        s_U2S_Distance_Km = h3.point_dist((float(row['planet_lat']), float(row['planet_long'])),(float(row['s_lat']), float(row['s_long'])))

    if pd.isnull(row['s_connected_devices']) or pd.isnull(row['t_connected_devices']) or row['s_connected_devices'] == '' or row['t_connected_devices'] == '':
        s2t_connected_devices_delta = None
    else:
        s2t_connected_devices_delta = row['s_connected_devices'] - row['t_connected_devices']

    if pd.isnull(row['s_prb_util_dl_r14']) or pd.isnull(row['t_prb_util_dl_r14']) or row['s_prb_util_dl_r14'] == '' or row['t_prb_util_dl_r14'] == '':
        s2t_prb_util_dl_delta = None
    else:
        s2t_prb_util_dl_delta = row['s_prb_util_dl_r14'] - row['t_prb_util_dl_r14']

    if pd.isnull(row['s_prb_util_ul_r14']) or pd.isnull(row['t_prb_util_ul_r14']) or row['s_prb_util_ul_r14'] == '' or row['t_prb_util_ul_r14'] == '':
        s2t_prb_util_ul_delta = None
    else:
        s2t_prb_util_ul_delta = row['s_prb_util_ul_r14'] - row['t_prb_util_ul_r14']

    if s_S2U_Bearing == None or pd.isnull(row['s_cell_bearing']) or pd.isnull(row['s_cell_beamwidth']) or row['s_cell_bearing'] == '' or row['s_cell_beamwidth'] == '':
        s_cell_S2U_Bearing_confirmed = 0
    else:
        if s_S2U_Bearing >= float(row['s_cell_bearing']) - float(row['s_cell_beamwidth']) / 2 and s_S2U_Bearing <= float(row['s_cell_bearing']) + float(row['s_cell_beamwidth']) / 2:
            s_cell_S2U_Bearing_confirmed = 1
        else:
            s_cell_S2U_Bearing_confirmed = 0

    if pd.isnull(row['t_S2U_Bearing']) or pd.isnull(row['t_cell_bearing']) or pd.isnull(row['t_cell_beamwidth']) or row['t_S2U_Bearing'] == '' or row['t_cell_bearing'] == '' or row['t_cell_beamwidth'] == '':
        t_cell_S2U_Bearing_confirmed = 0
    else:
        if float(row['t_S2U_Bearing']) >= float(row['t_cell_bearing']) - float(row['t_cell_beamwidth']) / 2 and float(row['t_S2U_Bearing']) <= float(row['t_cell_bearing']) + float(row['t_cell_beamwidth']) / 2:
            t_cell_S2U_Bearing_confirmed = 1
        else:
            t_cell_S2U_Bearing_confirmed = 0

    if pd.isnull(row['s_rsrp']) or pd.isnull(row['t_rsrp']) or row['s_rsrp'] == '' or row['t_rsrp'] == '':
        s2t_rsrp_delta = None
    else:
        s2t_rsrp_delta = row['s_rsrp'] - row['t_rsrp']

    return s_cell_edge_low, s_cell_edge_high, t_cell_edge_low, t_cell_edge_high, s_S2U_Bearing, s_U2S_Distance_Km, s2t_connected_devices_delta, s2t_prb_util_dl_delta, s2t_prb_util_ul_delta, s_cell_S2U_Bearing_confirmed, t_cell_S2U_Bearing_confirmed, s2t_rsrp_delta

def find_co_channel(df, pci, earfcn, rsrp, is_target):
    # print(df[['wntd_id', 's_cell', 's_rsrp', 's_pci', 's_earfcn', 't_cell', 't_rsrp', 't_pci', 't_earfcn']].sort_values(by=['t_earfcn'], ascending=True))
    # print(str(pci), str(earfcn), str(rsrp))
    worst_co_channel = -1000
    co_channel_delta = 1000
    for i in df.index:
        if df.loc[i, 't_earfcn'] == earfcn and df.loc[i, 't_pci'] != pci and not isinstance(df.loc[i, 't_rsrp'], str):
            if df.loc[i, 't_rsrp'] > worst_co_channel:
                worst_co_channel = df.loc[i, 't_rsrp']
                co_channel_delta = float(rsrp) - float(worst_co_channel)
    if is_target:
        if earfcn == df['s_earfcn'].tolist()[0] and not isinstance(df['s_rsrp'].tolist()[0], str):
            if df['s_rsrp'].tolist()[0] > worst_co_channel:
                worst_co_channel = df['s_rsrp'].tolist()[0]
                co_channel_delta = float(rsrp) - float(worst_co_channel)
    return co_channel_delta

def read_settings():
    try:
        settings = pd.read_excel('parameters settings.xlsx', header=1)
    except:
        print('Parameters settings file is missing in same tool folder')
        sys.exit()

    min_dl_prb_th = settings.loc[settings['Parameter name'] == 'Min DL PRB util']['Value'].tolist()[0]
    if str(min_dl_prb_th) == 'nan' or isinstance(min_dl_prb_th,str) or min_dl_prb_th < 0 or min_dl_prb_th > 100: min_dl_prb_th = 70

    min_ul_prb_th = settings.loc[settings['Parameter name'] == 'Min UL PRB util']['Value'].tolist()[0]
    if str(min_ul_prb_th) == 'nan' or isinstance(min_ul_prb_th,str) or min_ul_prb_th < 0 or min_ul_prb_th > 100: min_ul_prb_th = 70

    rsrp_reduction_limit_1 = settings.loc[settings['Parameter name'] == 'Good RSRP Range max reduction']['Value'].tolist()[0]
    if str(rsrp_reduction_limit_1) == 'nan' or isinstance(rsrp_reduction_limit_1,str) or rsrp_reduction_limit_1 > 0: rsrp_reduction_limit_1 = -6

    rsrp_reduction_limit_2 = settings.loc[settings['Parameter name'] == 'Critical RSRP Range max reduction']['Value'].tolist()[0]
    if str(rsrp_reduction_limit_2) == 'nan' or isinstance(rsrp_reduction_limit_2,str) or rsrp_reduction_limit_2 > 0: rsrp_reduction_limit_2 = -3

    good_rsrp = settings.loc[settings['Parameter name'] == 'Good RSRP Range']['Value'].tolist()[0]
    if str(good_rsrp) == 'nan' or isinstance(good_rsrp,str) or good_rsrp > -40 or good_rsrp < -120: good_rsrp = -90

    critical_rsrp = settings.loc[settings['Parameter name'] == 'Critical RSRP Range']['Value'].tolist()[0]
    if str(critical_rsrp) == 'nan' or isinstance(critical_rsrp,str) or critical_rsrp > -40 or critical_rsrp < -120: critical_rsrp = -96

    min_t_co_ch = settings.loc[settings['Parameter name'] == 'Min target Co Channel delta']['Value'].tolist()[0]
    if str(min_t_co_ch) == 'nan' or isinstance(min_t_co_ch,str) or min_t_co_ch > 1000 or min_t_co_ch < 0: min_t_co_ch = 7

    min_s_co_ch = settings.loc[settings['Parameter name'] == 'Min source Co Channel delta']['Value'].tolist()[0]
    if str(min_s_co_ch) == 'nan' or isinstance(min_s_co_ch,str) or min_s_co_ch > 1000 or min_s_co_ch < 0: min_s_co_ch = 7

    min_co_ch_gain = settings.loc[settings['Parameter name'] == 'Min Co Channel gain']['Value'].tolist()[0]
    if str(min_co_ch_gain) == 'nan' or isinstance(min_co_ch_gain,str) or min_co_ch_gain < 0: min_co_ch_gain = 3

    min_rsrp_gain_m3 = settings.loc[settings['Parameter name'] == 'Mode3 Min RSRP gain']['Value'].tolist()[0]
    if str(min_rsrp_gain_m3) == 'nan' or isinstance(min_rsrp_gain_m3, str) or min_rsrp_gain_m3 < 0: min_rsrp_gain_m3 = 3

    min_rsrp_gain_pl = settings.loc[settings['Parameter name'] == 'Planet Min RSRP gain']['Value'].tolist()[0]
    if str(min_rsrp_gain_pl) == 'nan' or isinstance(min_rsrp_gain_pl, str) or min_rsrp_gain_pl < 0: min_rsrp_gain_pl = 6

    min_rsrp_gain_eq_co_ch = settings.loc[settings['Parameter name'] == 'Equal CoChannel target min RSRP gain']['Value'].tolist()[0]
    if str(min_rsrp_gain_eq_co_ch) == 'nan' or isinstance(min_rsrp_gain_eq_co_ch, str) or min_rsrp_gain_eq_co_ch < 0: min_rsrp_gain_eq_co_ch = 3

    return min_dl_prb_th, min_ul_prb_th, rsrp_reduction_limit_1, rsrp_reduction_limit_2, good_rsrp, critical_rsrp, min_t_co_ch, min_s_co_ch, min_co_ch_gain, min_rsrp_gain_m3, min_rsrp_gain_pl, min_rsrp_gain_eq_co_ch

min_dl_prb_th, min_ul_prb_th, rsrp_reduction_limit_1, rsrp_reduction_limit_2, good_rsrp, critical_rsrp, min_t_co_ch, min_s_co_ch, min_co_ch_gain, min_rsrp_gain_m3, min_rsrp_gain_pl, min_rsrp_gain_eq_co_ch = read_settings()
def calculate_flags(row):
    # Azimuth flag
    if ((row['s_cell_S2U_Bearing_confirmed'] == 1) and (row['t_cell_S2U_Bearing_confirmed'] == 1)) or (row['s_cell_S2U_Bearing_confirmed'] == 0):
        azimuth_flag = 1
    else:
        azimuth_flag = 0

    # Distance flag
    if pd.isnull(row['t_U2S_Distance_Km']) or pd.isnull(row['s_U2S_Distance_Km']):
        distance_flag = 0
    elif row['t_U2S_Distance_Km'] <= row['s_U2S_Distance_Km']:
        distance_flag = 1
    else:
        distance_flag = 0

    # CoChannel Flag
    if row['data_source'] == 'mode3':
        if row['s_rsrp'] > good_rsrp:
            if row['wntd_version'] in ['V1', 'V2']:
                if (row['t_co_channel_delta'] > row['s_co_channel_delta'] and float(row['t_rsrp']) - float(row['s_rsrp']) >= rsrp_reduction_limit_1 and float(row['t_rsrp']) > good_rsrp and float(row['t_cinr']) > float(row['s_cinr'])) or (row['t_co_channel_delta'] == row['s_co_channel_delta'] and float(row['t_rsrp']) - float(row['s_rsrp']) >= min_rsrp_gain_eq_co_ch and float(row['t_cinr']) > float(row['s_cinr'])):
                    co_channel_flag = 1
                else:
                    co_channel_flag = 0
            else:
                if (row['t_co_channel_delta'] > row['s_co_channel_delta'] and float(row['t_rsrp']) - float(row['s_rsrp']) >= rsrp_reduction_limit_1 and float(row['t_rsrp']) > good_rsrp and float(row['t_rsrq']) > float(row['s_rsrq'])) or (row['t_co_channel_delta'] == row['s_co_channel_delta'] and float(row['t_rsrp']) - float(row['s_rsrp']) >= min_rsrp_gain_eq_co_ch and float(row['t_rsrq']) > float(row['s_rsrq'])):
                    co_channel_flag = 1
                else:
                    co_channel_flag = 0
        elif row['s_rsrp'] > critical_rsrp:
            if row['wntd_version'] in ['V1', 'V2']:
                if (row['t_co_channel_delta'] > row['s_co_channel_delta'] and float(row['t_rsrp']) - float(row['s_rsrp']) >= rsrp_reduction_limit_2 and float(row['t_rsrp']) > critical_rsrp and float(row['t_cinr']) > float(row['s_cinr'])) or (row['t_co_channel_delta'] == row['s_co_channel_delta'] and float(row['t_rsrp']) - float(row['s_rsrp']) >= min_rsrp_gain_eq_co_ch and float(row['t_cinr']) > float(row['s_cinr'])):
                    co_channel_flag = 1
                else:
                    co_channel_flag = 0
            else:
                if (row['t_co_channel_delta'] > row['s_co_channel_delta'] and float(row['t_rsrp']) - float(row['s_rsrp']) >= rsrp_reduction_limit_2 and float(row['t_rsrp']) > critical_rsrp and float(row['t_rsrq']) > float(row['s_rsrq'])) or (row['t_co_channel_delta'] == row['s_co_channel_delta'] and float(row['t_rsrp']) - float(row['s_rsrp']) >= min_rsrp_gain_eq_co_ch and float(row['t_rsrq']) > float(row['s_rsrq'])):
                    co_channel_flag = 1
                else:
                    co_channel_flag = 0
        else:
            if float(row['t_rsrp']) > float(row['s_rsrp']) and row['t_co_channel_delta'] > min_t_co_ch:
                co_channel_flag = 1
            else:
                co_channel_flag = 0
    else:
        if pd.isnull(row['s_rsrp']) or pd.isnull(row['t_rsrp']):
            co_channel_flag = 0
        elif round(row['t_rsrp'], 2) > round(row['s_rsrp'], 2):
            co_channel_flag = 1
        else:
            co_channel_flag = 0

    # Prb flag
    if pd.isnull(row['t_prb_util_dl_r14']) or pd.isnull(row['s_prb_util_dl_r14']):
        prb_flag = -1
    elif row['t_prb_util_dl_r14'] <= min_dl_prb_th and row['t_prb_util_ul_r14'] <= min_ul_prb_th:
        prb_flag = 1
    elif row['t_prb_util_dl_r14'] > row['s_prb_util_dl_r14'] or row['t_prb_util_ul_r14'] > row['s_prb_util_ul_r14']:
        prb_flag = -1
    else:
        prb_flag = 1

    # CC flag
    if row['wntd_version'] in ['V1', 'V2']:
        cc_flag = 1
    elif pd.isnull(row['s_dl_cc']) or pd.isnull(row['t_dl_cc']):
        cc_flag = -1
    elif int(row['t_dl_cc']) > 1:
        cc_flag = 1
    elif int(row['s_dl_cc']) > 1:
        cc_flag = -1
    else:
        cc_flag = 0

    # if pd.isnull(row['s_dl_cc']) or pd.isnull(row['t_dl_cc']):
    #     cc_flag = -1
    # elif int(row['t_dl_cc']) > int(row['s_dl_cc']):
    #     cc_flag = 1
    # elif int(row['t_dl_cc']) == int(row['s_dl_cc']):
    #     if pd.isnull(row['s_ul_cc']) or pd.isnull(row['t_ul_cc']):
    #         cc_flag = 0
    #     elif int(row['t_ul_cc']) > int(row['s_ul_cc']):
    #         cc_flag = 1
    #     else:
    #         cc_flag = 0
    # else:
    #     cc_flag = -1

    # valid move flag, co channel gain and rsrp gain
    if co_channel_flag == 1 and distance_flag == 1:
        if prb_flag != -1 and cc_flag != -1:
            if str(row['s_earfcn'])[0] != str(row['t_earfcn'])[0]:
                if row['wntd_version'] not in ['V1', 'V2']:
                    valid_move = 1
                    if row['data_source'] == 'mode3':
                        co_ch_gain = row['t_co_channel_delta'] - row['s_co_channel_delta']
                        rsrp_gain = float(row['t_rsrp']) - float(row['s_rsrp'])
                    else:
                        co_ch_gain = None
                        rsrp_gain = float(row['t_rsrp']) - float(row['s_rsrp'])
                else:
                    valid_move = 0
                    co_ch_gain = None
                    rsrp_gain = None
            else:
                valid_move = 1
                if row['data_source'] == 'mode3':
                    co_ch_gain = row['t_co_channel_delta'] - row['s_co_channel_delta']
                    rsrp_gain = float(row['t_rsrp']) - float(row['s_rsrp'])
                else:
                    co_ch_gain = None
                    rsrp_gain = float(row['t_rsrp']) - float(row['s_rsrp'])
        else:
            valid_move = 0
            co_ch_gain = None
            rsrp_gain = None
    else:
        valid_move = 0
        co_ch_gain = None
        rsrp_gain = None

    return co_channel_flag, distance_flag, azimuth_flag, prb_flag, cc_flag, valid_move, co_ch_gain, rsrp_gain

def get_moves(df):
    if df.t_co_channel_delta.max() >= min_t_co_ch:
        df = df.loc[df['t_co_channel_delta'] >= min_t_co_ch]
    df['flags_sum'] = df['azimuth_flag'] + df['prb_flag'] + df['cc_flag']
    # print(df[['wntd_id', 'rsrp_distance_azimuth_flag', 'prb_flag', 'cc_flag', 'valid_move', 'flags_sum']])
    if 3 in df['flags_sum'].tolist():
        df = df.loc[df['flags_sum'] == 3]
        if len(df) > 1:
            df = df.loc[df['t_rsrp'] == df.t_rsrp.max()]
            # print(df[['wntd_version', 'wntd_id', 't_rsrq', 't_cinr']])
            return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
        else:
            return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
    elif 2 in df['flags_sum'].tolist():
        df = df.loc[df['flags_sum'] == 2]
        if len(df) > 1:
            if 1 in df['azimuth_flag'].tolist():
                df = df.loc[df['azimuth_flag'] == 1]
                if len(df) > 1:
                    if 1 in df['cc_flag'].tolist():
                        df = df.loc[df['cc_flag'] == 1]
                        if len(df) > 1:
                            df = df.loc[df['t_rsrp'] == df.t_rsrp.max()]
                            return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
                        else:
                            return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
                    else:
                        df = df.loc[df['t_rsrp'] == df.t_rsrp.max()]
                        return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0],df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
                else:
                    return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
            else:
                df = df.loc[df['t_rsrp'] == df.t_rsrp.max()]
                return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
        else:
            return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
    else:
        df = df.loc[df['flags_sum'] == 1]
        if len(df) > 1:
            if 1 in df['azimuth_flag'].tolist():
                df = df.loc[df['azimuth_flag'] == 1]
                if len(df) > 1:
                    df = df.loc[df['t_rsrp'] == df.t_rsrp.max()]
                    return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
                else:
                    return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
            else:
                df = df.loc[df['t_rsrp'] == df.t_rsrp.max()]
                return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]
        else:
            return df['data_source'].tolist()[0], df['wntd_id'].tolist()[0], df['imsi'].tolist()[0], df['s_site'].tolist()[0], df['s_cell'].tolist()[0], df['s_pci'].tolist()[0], df['t_site'].tolist()[0], df['t_cell'].tolist()[0], df['t_pci'].tolist()[0], df['s_co_channel_delta'].tolist()[0], df['t_co_channel_delta'].tolist()[0], df['Co_channel_gain'].tolist()[0], df['s_rsrp'].tolist()[0], df['t_rsrp'].tolist()[0], df['rsrp_gain'].tolist()[0], df['s_cell_bearing'].tolist()[0], df['t_cell_bearing'].tolist()[0]

def save_xls(dict_df, path):
    writer = ExcelWriter(path)
    for key in dict_df.keys():
        dict_df[key].drop_duplicates().to_excel(writer, sheet_name=key, index=False)

    writer.close()


def get_site_codes(code, coordinates, filter_dist):
    coordinates.loc[:, 'ref_lat'] = coordinates.loc[coordinates['Code'] == code.upper()].iloc[0, 1]
    coordinates.loc[:, 'ref_long'] = coordinates.loc[coordinates['Code'] == code.upper()].iloc[0, 2]

    coordinates['actual_latitude'] = pd.to_numeric(coordinates['actual_latitude'], errors='coerce')
    coordinates['actual_longitude'] = pd.to_numeric(coordinates['actual_longitude'], errors='coerce')
    coordinates['ref_lat'] = pd.to_numeric(coordinates['ref_lat'], errors='coerce')
    coordinates['ref_long'] = pd.to_numeric(coordinates['ref_long'], errors='coerce')
    coordinates['Distance'] = coordinates.apply(lambda row: h3.point_dist((row['actual_latitude'], row['actual_longitude']), (row['ref_lat'], row['ref_long'])), axis=1)
    # coordinates.to_csv('coordinates.csv', index=False)
    coordinates_filtered = coordinates.loc[coordinates['Distance'] <= filter_dist]
    while len(coordinates_filtered['Code'].tolist()) == 1:
        filter_dist = filter_dist + 10
        coordinates_filtered = coordinates.loc[coordinates['Distance'] <= filter_dist]
    return "'" + """','""".join(coordinates_filtered['Code'].tolist()) + "'"
