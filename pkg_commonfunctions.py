# functions that can be shared with other ETL scripts

def strip_clean_drop(dataframe):
    dataframe.columns = dataframe.columns.str.strip()  # gets rid of extra spaces
    dataframe.columns = dataframe.columns.str.lower()  # converts to lower case
    dataframe.columns = dataframe.columns.str.replace(' ', '_')  # allow dot notation with no spaces
    dataframe.columns = dataframe.columns.str.replace("'", '')  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace("(", '', regex=False)  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace(")", '', regex=False)  # allow dot notation with no special chars
    dataframe.columns = dataframe.columns.str.replace(".", '', regex=False)  # allow dot notation with no special chars
    dataframe = dataframe.dropna(axis=0, how="all")
    dataframe = dataframe.dropna(axis=1, how="all")
    dataframe = dataframe.convert_dtypes(convert_string=True)
    
    try:
        dataframe = dataframe.apply(lambda x: x.str.strip() if x.dtypes=="string" else x)
    except:
        pass
    
    return dataframe

def calculate_age(df, birth_date_col):
    today = pd.Timestamp(date.today())
    age = (today - df[birth_date_col]).astype('timedelta64[D]') / 365.25
    age[df[birth_date_col].isna()] = 0
    return round(age, 2)

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


