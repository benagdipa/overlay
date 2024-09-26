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
    start = F.print_header("Running Mode 3 query....")
    try:
        start = F.print_activity("---Mode3 query running...")
        # mode3_df = pd.read_csv('queries/mode3_query.csv')
        mode3_df = pd.read_sql(m3_q.Mod3_query, conn)
        mode3_df.to_csv('queries/mode3_query.csv', index=False)
        F.print_done_activity(f'---Mode3 query completed ({len(mode3_df)} Records generated)', start)
    except:
        print("ERROR! no connection to THOR or error with the query run time")
        sys.exit()

    start = F.print_header("Running other support queries queries....")
    try:
        start = F.print_activity("---Production data query running...")
        # production_df = pd.read_csv('queries/production_query.csv')
        production_df = pd.read_sql(Q.production_query, conn)
        production_df.to_csv('queries/production_query.csv', index=False)
        F.print_done_activity('---Production data query completed', start)

        start = F.print_activity("---Users count query running...")
        # user_count_df = pd.read_csv('queries/user_count_query.csv')
        user_count_df = pd.read_sql(Q.user_count_query, conn)
        user_count_df.to_csv('queries/user_count_query.csv', index=False)
        F.print_done_activity('---Users count query completed', start)

    except:
        print("ERROR! no connection to THOR or missing views")
        sys.exit()


    dummy1 = F.print_header("\nProcess Mode3 data....")
    start = F.print_activity("---Removing single record users")
    # identify single record wntds
    mode3_locs = mode3_df[['location_id']]
    output_dict = {'Mode3 single entry LOCs': mode3_locs[~mode3_locs.duplicated(keep=False)]}

    # keep only wntds with more than one record
    multi_record_locs = mode3_locs[mode3_locs.duplicated(keep=False)]
    mode3_df = mode3_df.loc[mode3_df['location_id'].isin(multi_record_locs['location_id'].tolist())]
    mode3_df.loc[:, 'data_source'] = 'mode3'
    F.print_done_activity('---Removing single record users', start)
    return mode3_df, output_dict, '2'
