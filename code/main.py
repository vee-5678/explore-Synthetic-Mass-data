import pandas as pd
import numpy as np
from pathlib import Path
import os
import warnings

########################################################################################################################
# Setup
########################################################################################################################
# Display options
pd.set_option('display.width', 180)
pd.set_option('display.max_columns', 10)


########################################################################################################################
# Variables
########################################################################################################################
input_data_fp = Path('C:/', 'data', 'synthea_1m_fhir_3_0_May_24')
# Based on files modified, data extracted in 2017 (31/12 for simplicity); used to calculate age for pts with no deathdate
assumed_date_of_extract = '2017-12-31'

########################################################################################################################
# Functions
########################################################################################################################
def derive_age(birthdate, deathdate):
    """Simple function to calculate age and handle where patient is still alive"""
    end_date_to_use = pd.to_datetime(assumed_date_of_extract) if pd.isnull(deathdate) else deathdate
    age = ((end_date_to_use - birthdate) / np.timedelta64(1, 'Y'))
    # Round to 2 d.p.
    age = round(age, 2)
    
    return age


########################################################################################################################
# Import data
########################################################################################################################
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
            print(f'{file.parent.name}/{file.name} unable to be parsed:', e)
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
del patients_to_include

# Parse date fields
patients[['birthdate', 'deathdate']] = patients[['birthdate', 'deathdate']].apply(pd.to_datetime, errors='raise', format='%Y-%m-%d')
conditions[['start', 'stop']] = conditions[['start', 'stop']].apply(pd.to_datetime, errors='raise', format='%Y-%m-%d')
encounters['date'] = pd.to_datetime(encounters['date'], errors='raise', format='%Y-%m-%d')
medications[['start', 'stop']] = medications[['start', 'stop']].apply(pd.to_datetime, errors='raise', format='%Y-%m-%d')
procedures['date'] = pd.to_datetime(procedures['date'], errors='raise', format='%Y-%m-%d')

# Add year variables for looking at trends over time
# Where there is a start and stop date, just use start date for simplicty
conditions['year'] = conditions['start'].dt.year
encounters['year'] = encounters['date'].dt.year
medications['year'] = medications['start'].dt.year
procedures['year'] = procedures['date'].dt.year

# In procedures, "Documentation of current medications" seems to be very common, though likely a system / process requirement?
# Not very useful for analysis though, so dropping it
procedures = procedures.loc[procedures['description'].str.lower() != 'documentation of current medications']


# Check for any duplicates in patient 
duplicate_pt_ids = patients.loc[patients.duplicated(subset='id')]
if len(duplicate_pt_ids.index) > 0:
    warnings.warn('Duplicate IDs in patients DF')

duplicate_pt_other_fields = patients.loc[patients.duplicated(subset=['birthdate', 'first', 'last', 'ssn'])]
if len(duplicate_pt_other_fields.index) > 0:
    warnings.warn('Potentialy duplicates in patients DF')


########################################################################################################################
# Transform - add useful variables
########################################################################################################################
# Patient age, and age groups - set as date types first
patients['age'] = patients.apply(lambda x: derive_age(x['birthdate'], x['deathdate']), axis=1)
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

# Aggregated variables - by patient
# TODO: make this into a func - take in variable to count and variable to groupby
conditions_count_per_pt = conditions.groupby('patient')['code'].count().reset_index().rename(columns={'code': 'count_conditions'})
encounters_count_per_pt = encounters.groupby('patient')['id'].count().reset_index().rename(columns={'id': 'count_encounters'})
medications_count_per_pt = medications.groupby('patient')['code'].count().reset_index().rename(columns={'code': 'count_medications'})
procedures_count_per_pt = procedures.groupby('patient')['code'].count().reset_index().rename(columns={'code': 'count_procedures'})

# Join with patients DF
patients = patients.merge(
    right=conditions_count_per_pt,
    how='left',
    left_on='id',
    right_on='patient',
).merge(
    right=encounters_count_per_pt,
    how='left',
    left_on='id',
    right_on='patient'
).merge(
    right=medications_count_per_pt,
    how='left',
    left_on='id',
    right_on='patient'
).merge(
    right=procedures_count_per_pt,
    how='left',
    left_on='id',
    right_on='patient'
)
# Drop join keys from merging that are redundant and not required
patients = patients.drop(columns=['patient_x', 'patient_y'])


# Drop variables no longer required
del raw_data, conditions_count_per_pt, encounters_count_per_pt, medications_count_per_pt, procedures_count_per_pt




