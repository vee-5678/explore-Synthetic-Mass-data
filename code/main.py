import pandas as pd
import numpy as np
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

# Display options
pd.set_option('display.width', display_options_config.getint('desired_width'))
pd.set_option('display.max_columns', display_options_config.getint('max_columns'))

########################################################################################################################
# Functions
########################################################################################################################
def derive_age_group(age):


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

########################################################################################################################
# Tidy
########################################################################################################################
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

# Parse date fields
patients[['birthdate', 'deathdate']] = patients[['birthdate', 'deathdate']].apply(pd.to_datetime, errors='raise', format='%Y-%m-%d')
conditions[['start', 'stop']] = conditions[['start', 'stop']].apply(pd.to_datetime, errors='raise', format='%Y-%m-%d')
encounters['date'] = pd.to_datetime(encounters['date'], errors='raise', format='%Y-%m-%d')
medications[['start', 'stop']] = medications[['start', 'stop']].apply(pd.to_datetime, errors='raise', format='%Y-%m-%d')
procedures['date'] = pd.to_datetime(procedures['date'], errors='raise', format='%Y-%m-%d')


########################################################################################################################
# Transform
########################################################################################################################
# Create useful variables
# Patient age, and age groups - set as date types first
patients['age'] = round((patients['deathdate'] - patients['birthdate']) / np.timedelta64(1, 'Y'), 2)
patients['age_group'] = pd.cut(patients['age'],
                               # Could use pd.interval_range though gives misleading labels which have overlapping values
                               bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, float('inf')],
                               labels = ['<10', '10-19', '20-29', '30-39', '40-49', '50-59', '60-69', '70-79', '80-89', '90-99', '100+'],
                               right = False)

# Assumption that if no deathdate, then patient is still alive
patients['vital_status'] = patients.apply(lambda x: 'Alive' if pd.isnull(x['deathdate']) else 'Dead', axis=1)

# Length of medication prescription
medications['medication_duration'] = round((medications['stop'] - medications['start']) / np.timedelta64(1, 'D'), 2)

# Length of condition
conditions['condition_duration'] = round((conditions['stop'] - conditions['start']) / np.timedelta64(1, 'D'), 2)



