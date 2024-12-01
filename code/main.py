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
    # All CSVs named the same, just in different folders, so get the stem of the file which will indicate which DF
    data_category = file.stem
    if data_category not in data_of_interest:
        pass
    else:
        try:
            df = pd.read_csv(file)
            # Add in a col of which folder the data came from
            df['source_folder'] = file.parent.name
            # Add to the appropriate DF based on the name of the file
            raw_data[data_category] = df.copy() if raw_data[data_category].empty else pd.concat([raw_data[data_category], df])
        except pd.errors.ParserError as e:
            print("File unable to be parsed:", e)
            pass










