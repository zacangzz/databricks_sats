__author__ = "Zac Ang"
__email__ = "zhaozong.ang@gmail.com"
"""
pkg_commonfunctions.py
Functions that can be shared with other ETL scripts.
Imported by init.
"""

import pandas as pd
import re
import janitor
from datetime import date

# function to mangle duplicate column names
def mangle_dupe_cols(columns):
    counts = {}
    for i, col in enumerate(columns):
        cur_count = counts.get(col, 0)
        if cur_count > 0:
            columns[i] = f'{col}.{cur_count}'
        counts[col] = cur_count + 1
    return columns

# function to find duplicated columns
def find_duplicated_columns(columns):
    counts = {}
    duplicated_columns = []
    for col in columns:
        counts[col] = counts.get(col, 0) + 1
        if counts[col] > 1:
            duplicated_columns.append(col)
    return duplicated_columns

# duplicates = find_duplicated_columns(column_names)
# if duplicates:
#    raise ValueError(f"Duplicated columns detected: {', '.join(duplicates)}")

# function to encode strings as ASCII
def encode_strings(val):
    if isinstance(val, str):
        return val.encode('ascii', 'ignore').decode('ascii')
    else:
        return val

def strip_clean_drop(dataframe):
    dataframe.columns = dataframe.columns.str.strip()  # gets rid of extra spaces
    dataframe.columns = dataframe.columns.str.lower()  # converts to lower case
    dataframe.columns = dataframe.columns.str.replace(' ', '_')  # allow dot notation with no spaces
    dataframe.columns = dataframe.columns.str.replace('-', '_')  # allow dot notation with no spaces
    dataframe.columns = dataframe.columns.str.replace("'", '')  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace(":", '', regex=False)  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace("(", '', regex=False)  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace(")", '', regex=False)  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace(".", '', regex=False)  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace("\n", '_', regex=False)  # allow dot notation with no special chars
    dataframe = dataframe.dropna(axis=0, how="all")
    dataframe = dataframe.dropna(axis=1, how="all")
    dataframe = dataframe.convert_dtypes(convert_string=True)

    try:
        dataframe = dataframe.apply(lambda x: x.str.strip() if x.dtypes=="string" else x)
    except:
        pass

    dataframe = (
            dataframe.clean_names()
            .convert_dtypes()
            .reset_index(drop=True)
        ) # using pyjanitor 
    
    # Apply this function to the DataFrame's columns
    # dataframe.columns = mangle_dupe_cols(list(dataframe.columns))

    return dataframe

# converts everything to float first, then convert back to int, to get rid of any decimals
def convert_toInt_toStr(row, col):
    # print(f'Starting value is {row[col]}, with Type: {type(row[col])}')
    try:
        # print("Trying...")
        a = str(round(float(row[col])))
        return a
    except:
        return str(row[col])

# cleans up employee IDs to type consistency
def clean_eid(df):
    df['persno'] = df.apply(lambda row: convert_toInt_toStr(row, 'persno'), axis=1)
    df['persno'] = df['persno'].astype('string')
    df['reporting_officer'] = df.apply(lambda row: convert_toInt_toStr(row, 'reporting_officer'), axis=1)
    df['reporting_officer'] = df['reporting_officer'].astype('string')

    return df
    
def calculate_age(df, birth_date_col):
    today = pd.Timestamp(date.today())
    age = (today - df[birth_date_col]).astype('timedelta64[D]') / 365.25
    age[df[birth_date_col].isna()] = 0
    return round(age, 2)

# get age based on date, input which col to use as date, and whether to use today as the base or use predefine formula
def calculate_age(df, birth_date='birth_date', output_col='age', today=False):
    if today:
        today = pd.Timestamp(date.today())
    else:
        today = df['eom'].where(df['date_left'].isna(), df['date_left'])
        
    birth_date = df[birth_date]

    df[output_col] = ((today - birth_date).dt.days / 365.25).fillna(0).astype(int)
    df.loc[birth_date.isna(), output_col] = 0

    return df

def create_manager_dict(df):
    manager_dict = df.set_index('persno')['reporting_officer'].to_dict()
    return manager_dict

# calculate layer order - level analysis
def calculate_layers_iterative(emp_id, manager_dict, root_id='251747'):
    depth = 0
    current_id = emp_id

    while current_id != root_id:
        if current_id not in manager_dict or pd.isnull(manager_dict[current_id]):
            return None  # or depth, based on how you want to handle this case
        current_id = manager_dict[current_id]
        depth += 1

    return depth

# this is the dictionary to map months to numbers
month_no_dict = {
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
    "jan": 1,
    "feb": 2,
    "mar": 3,
}

# convert timestamps in unit format to pandas datetime
def convert_unix_timestamp(date_str):
    if pd.isnull(date_str):
        return pd.NaT
    timestamp = int(re.search(r'\d+', date_str).group())  # extract numeric part
    return pd.to_datetime(timestamp, unit='ms')  # convert Unix timestamp to datetime

# custom function to parse dates in 2 main formats: sharepoint csv files store in these 2 format for todays_date
def parse_date(date_str):
    for fmt in ('%d/%m/%Y', '%Y-%m-%d'):
        try:
            return pd.to_datetime(date_str, format=fmt)
        except ValueError:
            continue
    return pd.NaT  # Return Not a Time for unparseable formats

# check for Managerial Grade & replace it with H1-9
def check_pers_subarea(row):
    if "MANAGERIAL GRADE" in row['personnel_subarea']:
        if pd.isna(row['pay_scale_group']) or ("Def for NA" in row['pay_scale_group']) or ("OTHERS" in row['pay_scale_group']):
            return row['personnel_subarea']
        else:
            return row['pay_scale_group']
    else:
        return row['personnel_subarea']
    
    return

def cat_fyear(row, mthcol, yearcol):
    try:
        if row[mthcol] < 4:
            right_n = int(str(row[yearcol])[2:4])
            left_n = right_n - 1
            fyear = str(left_n) + str(right_n)
        else:
            left_n = int(str(row[yearcol])[2:4])
            right_n = left_n + 1
            fyear = str(left_n) + str(right_n)
        return fyear
    except:
        pass

# Clean up column data types
def set_dtypes(df):
    df["date_joined"] = pd.to_datetime(df["date_joined"], dayfirst=True, format='%d/%m/%Y', errors="coerce")
    df["birth_date"] = pd.to_datetime(df["birth_date"], dayfirst=True, format='%d/%m/%Y', errors="coerce")
    df["todays_date"] = df['todays_date'].apply(parse_date)
    '''df['company_code'] = df['company_code'].astype('category')
    df['organizational_unit'] = df['organizational_unit'].astype('category')
    df['employee_group'] = df['employee_group'].astype('category')
    df['personnel_subarea'] = df['personnel_subarea'].astype('category')
    df['gender_key'] = df['gender_key'].astype('category')
    df['nationality'] = df['nationality'].astype('category',errors='ignore')
    df['employee_sub_group'] = df['employee_sub_group'].astype('category',errors='ignore')
    df['fund_type'] = df['fund_type'].astype('category',errors='ignore')
    df['marital_status'] = df['marital_status'].astype('category',errors='ignore')
    df['pay_scale_group'] = df['pay_scale_group'].astype('category',errors='ignore')
    df['work_schedule'] = df['work_schedule'].astype('category',errors='ignore')
'''
    # define & convert string columns
    string_cols = [
        'persno',
        'cost_center',
        'position',
        'job',
        'zip_code'
    ]
    # convert to string if column exists
    for col in string_cols:
        if col in df.columns:
            print(f'checking: {col}')
            df[col] = df[col].astype('string',errors='ignore')

    # define & convert category columns
    category_cols = [
        'company_code',
        'organizational_unit',
        'employee_group',
        'personnel_subarea',
        'gender_key',
        'nationality',
        'employee_sub_group',
        'fund_type',
        'marital_status',
        'pay_scale_group',
        'work_schedule'
    ]
    # convert to category if column exists
    for col in category_cols:
        if col in df.columns:
            print(f'checking: {col}')
            df[col] = df[col].astype('category',errors='ignore')
    
    # define date columns
    date_cols = [
        "position_entry_date", 
        "confirmation_date", 
        "probation_end_date", 
        "probation_extension_end_date", 
        "contract_end_date", 
        "leave_eligibility_start_date",
        "date_left",
        "service_date"
    ]
    # convert to date if column exists
    for col in date_cols:
        if col in df.columns:
            print(f'checking: {col}')
            df[col] = pd.to_datetime(df[col], dayfirst=True, format='%d/%m/%Y', errors="coerce")

    # get joined date splits, all must have join dates
    df['_datejoined_month'] = df['date_joined'].dt.month.astype('Int64', errors='ignore')
    df['_datejoined_year'] = df['date_joined'].dt.year.astype('str', errors='ignore')
    df['_datejoined_fy'] = df.apply(lambda row: cat_fyear(row,'_datejoined_month','_datejoined_year'), axis=1)

    # get termination date splits if date_left exists
    if 'date_left' in df.columns:
        df['_dateleft_month'] = df['date_left'].dt.month.astype('Int64', errors='ignore')
        df['_dateleft_year'] = df['date_left'].dt.year.astype('str', errors='ignore')
        df['_dateleft_fy'] = df.apply(lambda row: cat_fyear(row,'_dateleft_month','_dateleft_year'), axis=1)
    
    return df