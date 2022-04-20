import pandas as pd
import matplotlib.pyplot as plt

def is_number(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def format_to_numeric(df, row_name):
    df[row_name] = df[row_name].apply(lambda x: pd.to_numeric(x, errors="coerce"))
    df.dropna(subset=[row_name], inplace=True)

# Read CSV dataset
df = pd.read_csv("weather_dataset.csv", encoding="utf-8")

### Exploration
print("Data exploration...")
print("\nFirst five rows : ")
print(df.head())
print("\nShape (rows, columns ) : ")
print(df.shape)
print("\nDataset info : ")
print(df.info())
print("\nColumn with mixed types :")
for col in df.columns:
    weird = (df[[col]].applymap(type) != df[[col]].iloc[0].apply(type)).any(axis=1)
    if len(df[weird]) > 0:
        print(col)

# Clean rows with mixed types (delete all rows that contains NaN value)
print("\nReformating and cleaning dataset...")
format_to_numeric(df, "Visibility")
format_to_numeric(df, "WindSpeed")
format_to_numeric(df, "DryBulbFarenheit")
format_to_numeric(df, "DryBulbCelsius")
format_to_numeric(df, "WetBulbFarenheit")
format_to_numeric(df, "WetBulbCelsius")
format_to_numeric(df, "DewPointFarenheit")
format_to_numeric(df, "DewPointCelsius")
format_to_numeric(df, "RelativeHumidity")
format_to_numeric(df, "StationPressure")
format_to_numeric(df, "SeaLevelPressure")
format_to_numeric(df, "Altimeter")

# Convert RelativeHumidity to float
df = df.astype({"RelativeHumidity": float})

# Add column isRain
df["isRain"] = 0
for row in df.itertuples():
    # If precipitation then isRain = 1
    if is_number(df.at[row.Index, "HourlyPrecip"]):
        df.at[row.Index, "isRain"] = 1

# Show dataset schema
print("\nDataset schema")
print(df.dtypes)

# Save
print(f"\nDataset saved in weather_dataset_clean.csv")
df.to_csv("weather_dataset_clean.csv", index=False, encoding="utf-8")

# Check if the dataset is unbalanced or balanced
fig = plt.figure(figsize = (8,5))
df.isRain.value_counts(normalize = True).plot(kind='bar', color= ['skyblue','navy'], alpha = 0.9, rot=0)
plt.title('isRain 0 and 1 indicators in the imbalanced dataset')
plt.show()























