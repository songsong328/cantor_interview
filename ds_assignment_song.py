# -*- coding: utf-8 -*-
"""
Created on Sun May 16 14:54:33 2021

@author: songw
"""

import pandas as pd


def timestamp_mapping(year, month, day, time):
    year = str(year)
    month_mapper = {'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12}
    month = month_mapper[month]
    day = str(day)

    return pd.to_datetime('{year}-{month}-{day} {time}'.format(year=year,
                                                               month=month,
                                                               day=day,
                                                               time=time))
    
    
path = r"C:\Users\songw\Downloads\DS Assigment\\"

# load input file
data_raw = ""
input_name = "corp_pfd.dif"
with open(path + input_name) as f:
    data_raw = f.readlines()

# 0. clean input texts by taking out new lines, # with a space, and empty lines
data_raw = [x.replace('\n', '') for x in data_raw if (x[:2] != '# ') & (x.replace('\n', '') != '')]

# 1. find the field name blocks
field_start = 'START-OF-FIELDS'
field_end = 'END-OF-FIELDS'
field_name_idx = [i for i, _ in enumerate(data_raw) if (field_start in _) | (field_end in _)]
field_names = data_raw[field_name_idx[0]+1 : field_name_idx[1]]

# 2.1 find the data blocks
data_start = 'START-OF-DATA'
data_end = 'END-OF-DATA'
data_idx = [i for i, _ in enumerate(data_raw) if (data_start in _) | (data_end in _)]
data = data_raw[data_idx[0]+1 : data_idx[1]]

# 2.2 split the | delimited data and remove last cell
data_splited = [x.split('|')[:-1] for x in data]

# 3. create data frame out of the found blocks
# data records match to provided 2896
df_raw = pd.DataFrame(columns=field_names,
                      index=range(len(data)),
                      data=data_splited)

# 4. limit columns to the ones in reference_fields.csv
ref_fields = pd.read_csv(path + "reference_fileds.csv")
sub_fields = df_raw.columns[df_raw.columns.isin(ref_fields['field'].tolist())]
df = df_raw[sub_fields]

# 5. in reference_securities (file)
"""
1. use ID_BB_GLOBAL as key
2. left join the file and take out the shared rows
3. drop duplicated rows and keep the first record index-wise
4. keep the same structure as they are in file
"""

ref_secs = pd.read_csv(path + "reference_securities.csv")
merged = df.merge(ref_secs,
                  left_on=['ID_BB_GLOBAL'], right_on=['id_bb_global'],
                  how='left')

# 5.1 take out shared keys
new_secs = df[~df['ID_BB_GLOBAL'].isin(ref_secs['id_bb_global'].unique())]

# 5.2 althrough only 1 record, still de-dupe for future
new_secs.drop_duplicates(subset=['ID_BB_GLOBAL'], keep='first', inplace=True)

# 5.3 keep the file's format (checked no column in and outs)
new_secs.columns = new_secs.columns.str.lower()
new_ref_secs = new_secs[ref_secs.columns]

# 6. create Field-Value reference
# each security should have 50 - 1 = 49 rows because data has 49 non-id fields
value_vars = [x for x in df.columns if (x != 'ID_BB_GLOBAL')]
sec_data = df.melt(id_vars=['ID_BB_GLOBAL'], value_vars=value_vars,
                   var_name='FIELD', value_name='VALUE')
sec_data['SOURCE'] = input_name
tstamp_row = [i for i, _ in enumerate(data_raw) if 'TIMEFINISHED' in _][0]
tstamp_row_token = data_raw[tstamp_row].split(' ')
year, month, day, time = tstamp_row_token[-1], tstamp_row_token[1], tstamp_row_token[3], tstamp_row_token[4]
sec_data['TSTAMP'] = timestamp_mapping(year, month, day, time)

# 7. save the 2 output tables
new_ref_secs.to_csv(path + "new_securities.csv", index=False)
sec_data.to_csv(path + "security_data.csv", index=False)
