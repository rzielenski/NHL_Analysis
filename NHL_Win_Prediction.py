import pandas as pd
import joblib
import numpy as np
from tensorflow import keras
from sklearn.preprocessing import StandardScaler
from NHL_Game_Retriever import *
import requests


def get_WP(home_team, away_team):

    # Load the trained model
    model = keras.models.load_model(r'NHL_Win_Prediction.keras')

    team_data_path = r'C:\Users\Richard\OneDrive\Documentos\CS465_Project\2024_2025_All_Teams_Data_TEST.csv'
    team_data = pd.read_csv(team_data_path)

    df = pd.read_csv('Curr_Teams_For_NN.csv')
    df.drop(columns=["GF", "GA"])
    df_home = df.loc[df['Team'] == home_team].iloc[0]
    df_away = df.loc[df['Team'] == away_team].iloc[0]
    
    df_home_new = df_home.drop('Team').add_prefix('Home_')
    df_away_new = df_away.drop('Team').add_prefix('Away_')

    x = pd.DataFrame([pd.concat([df_home_new, df_away_new])])
    print(x)
    scaler = joblib.load(r'scaler.pkl')
    X_new_scaled = scaler.transform(x)

    y_pred_prob = model.predict(X_new_scaled)

    print(float(y_pred_prob[0][0]))
    return float(y_pred_prob[0][0])
