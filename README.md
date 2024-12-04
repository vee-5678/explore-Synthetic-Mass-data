# Clean and explore SyntheticMass data 📊
Repository set up to clean, explore, and visualise SyntheticMass data (generated by Synthea). This will help facilitate understanding of the data to gain insights, help with further investigation, and for decision-making.

## The data
The dataset was [SyntheticMass data](https://synthea.mitre.org/downloads) as generated by [Synthea](https://github.com/synthetichealth/synthea). The data contains synthetic patient records and simulated encounters, conditions, medications, and procedures. The full dataset was used, though due to data quality issues, only some records were used in analysis.

## Code
There are 3 key code elements:
- A Python script for importing, cleaning, tidying, and transforming of the data. This cleaned dataset forms the foundation for the other scripts.
- A Jupyter notebook for initial exploration of the data in the form of simple charts and basic statistical analysis of numerical variables.
- A Jupyter notebook using PyGWalker to provide interactive visualisations to aid further exploration of the data. Some insights and saved charts are provided.

## Installation

### Prerequisites
- Python 3.11+
- Libraries (see requirements.txt): `pandas`, `numpy`, `plotly-express`, `pygwalker`, `pathlib`

### Steps
1. Clone the repository:
   ```bash
   git clone https://github.com/vee-5678/explore-Synthetic-Mass-data.git

2. Source the data from [SyntheticMass](https://synthea.mitre.org/downloads), unzip the dataset, and update the _input_data_fp_ variable in main.py to point to the directory of unzipped data.

3. Either run main.py for dataset cleaning, OR open Jupyter notebook of interest and run the cells / notebook. PyGWalker visualisations are interactive, and can be saved.




