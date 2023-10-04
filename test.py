import pandas as pd
import random
import numpy as np

# Define the number of rows in your dataset
num_rows = 100

# Create a list of possible values for gender
genders = ['M', 'F']

# Create an empty DataFrame
data = pd.DataFrame(columns=[
    'id', 'UserAge', 'event_id', 'session_id', 'user_id',
    'gender', 'genderProbability', 'angry', 'disgusted',
    'fearful', 'happy', 'neutral', 'sad', 'surprised', 'state',
    'currentTime', 'ip', 'created_at', 'updated_at'
])

# Generate random data for each column
for i in range(num_rows):
    data.loc[i] = [
        i + 1,  # id
        random.randint(18, 70),  # UserAge
        random.randint(1, 100),  # event_id
        random.randint(1, 10),  # session_id
        random.randint(1000, 9999),  # user_id
        random.choice(genders),  # gender
        round(random.uniform(0, 1), 2),  # genderProbability
        random.randint(0, 100),  # angry
        random.randint(0, 100),  # disgusted
        random.randint(0, 100),  # fearful
        random.randint(0, 100),  # happy
        random.randint(0, 100),  # neutral
        random.randint(0, 100),  # sad
        random.randint(0, 100),  # surprised
        random.randint(0, 100),  # state
        round(random.uniform(0, 600), 2),  # currentTime
        f'192.168.{random.randint(0, 255)}.{random.randint(0, 255)}',  # ip
        '2023-10-04 12:00:00',  # created_at
        '2023-10-04 12:00:00'  # updated_at
    ]

# Display the first few rows of the generated dataset
print(data.head())

import seaborn as sns
import matplotlib.pyplot as plt

# Assuming you have a DataFrame named 'data' containing your data

# Create a pairplot to visualize relationships between numeric variables
sns.pairplot(data, hue='gender', diag_kind='kde')
plt.show()

# Create a bar plot to visualize gender distribution
sns.countplot(x='gender', data=data)
plt.show()

# Create a box plot to visualize the distribution of user ages by gender
sns.boxplot(x='gender', y='UserAge', data=data)
plt.show()

# Create a heatmap to visualize correlations between numeric variables
corr_matrix = data.corr()
sns.heatmap(corr_matrix, annot=True, cmap='coolwarm')
plt.show()
