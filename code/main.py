import pandas as pd
from pathlib import Path
import os
import configparser

########################################################################################################################
# Setup - read in config file
########################################################################################################################
config = configparser.ConfigParser()
config.read(Path(Path(os.getcwd()), 'config', 'config.ini'))

display_options_config = config['display_options']
file_path_config = config['file_paths']

# display options
pd.set_option('display.width', display_options_config.getint('desired_width'))
pd.set_option('display.max_columns', display_options_config.getint('max_columns'))


########################################################################################################################
# Import data
########################################################################################################################
input_data_fp = Path(file_path_config['input_data_fp'])

raw_data_file_list = [f for f in input_data_fp.glob('**/*') if f.is_file()]

data_of_interest = ['patients',
                    'conditions',
                    'encounters',
                    'medications',
                    'procedures',
                    'organizations']


raw_data = {'patients': pd.DataFrame(),
            'conditions': pd.DataFrame(),
            'encounters': pd.DataFrame(),
            'medications': pd.DataFrame(),
            'procedures': pd.DataFrame(),
            'organizations': pd.DataFrame()}

for file in raw_data_file_list:
    print(file)
    # CSVs named the same, just different folders, so get the stem of the file which will indicate which DF to concat to
    data_category = file.stem
    if data_category not in data_of_interest:
        pass
    else:
        try:
            df = pd.read_csv(file, header=0, index_col=False)
            # Add in a col of which folder the data came from (in case need to investigate source data)
            df['source_folder'] = file.parent.name
            # Add to the appropriate DF based on the name of the file
            raw_data[data_category] = df.copy() if raw_data[data_category].empty else pd.concat([raw_data[data_category], df])
        except pd.errors.ParserError as e:
            print("File unable to be parsed:", e)
            pass

# Standardise column names across all datasets - lowercase
for df in raw_data.values():
    df.columns = [col.lower() for col in df.columns]

# Get data out as basic DF once repeated, generic steps are complete
patients = raw_data.get('patients').copy(deep=True)
conditions = raw_data.get('conditions').copy(deep=True)
encounters = raw_data.get('encounters').copy(deep=True)
medications = raw_data.get('medications').copy(deep=True)
procedures = raw_data.get('procedures').copy(deep=True)
organizations = raw_data.get('organizations').copy(deep=True)

# Invalid data where something's gone wrong with field separation -  needs to be fixed at source
# Dropping rows with issues - preferentially using fields that are required (according to data dict) and with a pattern to help filter
patients = patients.loc[(pd.notnull(patients['ssn'])) & (patients['ssn'].str.startswith('999'))]
patients = patients.loc[(pd.notnull(patients['birthplace'])) & (patients['birthplace'].str.contains('US'))]

# As patient dataset is the focus of exploration, filtering down other DFs to where there is a pt in the patients DF
# Also drop any exact duplicates
patients_to_include = patients.id.to_list()
conditions = conditions.loc[conditions['patient'].isin(patients_to_include)].drop_duplicates()
encounters = encounters.loc[encounters['patient'].isin(patients_to_include)].drop_duplicates()
medications = medications.loc[medications['patient'].isin(patients_to_include)].drop_duplicates()
procedures = procedures.loc[procedures['patient'].isin(patients_to_include)].drop_duplicates()







