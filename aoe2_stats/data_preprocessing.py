#%%
import pandas as pd
import os
from sklearn.preprocessing import StandardScaler



from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer, make_column_transformer
from sklearn.ensemble import RandomForestClassifier, VotingClassifier, AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
import xgboost as xgb
from sklearn.model_selection import train_test_split, GridSearchCV, ParameterGrid, cross_validate
from sklearn.metrics import ConfusionMatrixDisplay, confusion_matrix
from sklearn import set_config
set_config(display="diagram")



os.chdir('/Users/jakegearon/CursorProjects/aoe2_stats')

# Load datasets
aoe_data = pd.read_csv('data/aoe_data.csv')
buildings_data = pd.read_csv('data/building_masterdata.csv', delimiter=';')
research_data = pd.read_csv('data/research_masterdata.csv', delimiter=';')

# Drop the 'Unnamed: 0' column in aoe_data as it is likely an index column from the original data
aoe_data.drop('Unnamed: 0', axis=1, inplace=True)

# Forward fill missing position values using .ffill() to avoid FutureWarning
position_cols = ['p1_xpos', 'p2_xpos', 'p1_ypos', 'p2_ypos']
aoe_data[position_cols] = aoe_data[position_cols].ffill()

aoe_df['diff_x'] = aoe_df['p1_xpos'] - aoe_df['p2_xpos']
aoe_df['diff_y'] = aoe_df['p1_ypos'] - aoe_df['p2_ypos']
aoe_df['diff_p1_p2'] = np.sqrt(aoe_df['diff_x']**2 + aoe_df['diff_y']**2)
numeric_features = ['duration','elo','diff_x','diff_y', 'diff_p1_p2']
categorical_features = ['map','p1_civ','p2_civ']

X = aoe_df[numeric_features + categorical_features]
aoe_df['winner_cat'] = aoe_df['winner'].apply(lambda x: 'player 1' if x == 0 else 'player 2') 
y = aoe_df['winner']
X_train, X_test, y_train, y_test = train_test_split(X, y, 
                                                    test_size=0.2, 
                                                    stratify=y, 
                                                    shuffle=True,
                                                    random_state=1234)
# One-hot encoding for categorical variables in aoe_data
categorical_cols = ['map', 'map_size', 'dataset', 'difficulty', 'p1_civ', 'p2_civ']
aoe_data = pd.get_dummies(aoe_data, columns=categorical_cols)

# Normalize numerical columns in aoe_data
scaler = StandardScaler()
numerical_cols = ['duration', 'elo'] + position_cols
aoe_data[numerical_cols] = scaler.fit_transform(aoe_data[numerical_cols])

# One-hot encoding for categorical variables in research_data
research_categorical_cols = ['Type', 'Technology', 'Building']
research_data = pd.get_dummies(research_data, columns=research_categorical_cols)

# Remove unnecessary 'Unnamed' columns from research_data
research_data = research_data.loc[:, ~research_data.columns.str.contains('^Unnamed')]

# Print DataFrame information to verify changes
print(aoe_data.info())
print(buildings_data.info())
print(research_data.info())

# Save the final preprocessed data in Parquet format
aoe_data.to_parquet('data/final_preprocessed_aoe_data.parquet', index=False)
buildings_data.to_parquet('data/final_preprocessed_buildings_data.parquet', index=False)
research_data.to_parquet('data/final_preprocessed_research_data.parquet', index=False)
# %%
import pandas as pd
import os

# Step 1: Ingesting the Data
directory = '/Users/jakegearon/CursorProjects/aoe2_stats/data/snapshots'
all_files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith('.csv')]

# Initialize an empty list to store dataframes
all_data = []

for file in all_files:
    # Extract timeslice from filename

    timeslice = int(file.split('_')[-1].replace('.csv', ''))
    df = pd.read_csv(file)
    df['timeslice'] = timeslice
    all_data.append(df)

# Combine all dataframes into a single dataframe
snapshots_data = pd.concat(all_data, ignore_index=True)

# Step 2: Optionally create a multi-index
snapshots_data.set_index(['match_id', 'timeslice'], inplace=True)

# Step 3: Storing as Parquet with partitioning by 'timeslice'
snapshots_data.to_parquet('data/final_preprocessed_snapshots_data.parquet', partition_cols=['timeslice'], index=False)

# %%
