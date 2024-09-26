
import pandas as pd

def process_wntd_data(file_path):
    '''
    Process the uploaded WNTD data file.
    Supports CSV and Excel formats.
    '''
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(file_path)
    else:
        raise ValueError("Unsupported file format")

    # Validate required columns
    required_columns = {'wntd_id', 'imsi', 's_rsrp', 't_rsrp', 's_cinr', 't_cinr', 's_rsrq', 't_rsrq'}
    if not required_columns.issubset(set(df.columns)):
        missing = required_columns - set(df.columns)
        raise ValueError(f"Missing columns: {', '.join(missing)}")

    # Calculate deltas (improvements between serving and target cells)
    df['rsrp_improvement'] = df['t_rsrp'] - df['s_rsrp']
    df['rsrq_improvement'] = df['t_rsrq'] - df['s_rsrq']
    df['cinr_improvement'] = df['t_cinr'] - df['s_cinr']

    # Return a DataFrame with required output columns
    output_df = df[['wntd_id', 'imsi', 'wntd_version', 's_cell', 't_cell', 's_rsrp', 't_rsrp', 'rsrp_improvement', 's_rsrq', 't_rsrq', 'rsrq_improvement', 's_cinr', 't_cinr', 'cinr_improvement']]
    return output_df

def compare_wntd_data(df):
    '''
    Compare serving and scanned cells based on RSRP, RSRQ, and CINR.
    Returns a DataFrame with improvement metrics.
    '''
    comparison_df = df[['wntd_id', 'rsrp_improvement', 'rsrq_improvement', 'cinr_improvement']]
    return comparison_df
