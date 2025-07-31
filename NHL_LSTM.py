import numpy as np
import tensorflow as tf
import pandas as pd
import requests
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Input, LSTM, Dense, Multiply, Softmax, Lambda, Dropout, Masking
from tensorflow.keras.models import Model
from tensorflow.keras.preprocessing.sequence import pad_sequences
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping
from tensorflow.keras.layers import Masking
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
from sklearn.utils.class_weight import compute_class_weight
from sklearn.model_selection import train_test_split
import joblib
from NHL_Game_Retriever import get_all_game_scores
from connect_to_database import get_players

def time_to_float(time_str):
    minutes, seconds = map(int, time_str.split(":"))
    return minutes + seconds / 60


def get_season_data(year):
  df = pd.DataFrame(columns=["TOI", "TeamRating", "OpponentRating", "xToi", "xShots", "xShotPercentage", "xShotAttempts"])
  labels = pd.Series(columns=["shots"])
  players = get_players(year)

  for player in players:
    p_url = f"https://api-web.nhle.com/v1/player/{player[0]}/game-log/{year}{year+1}/2"
    s_url = f"https://api-web.nhle.com/v1/player/{player[0]}/landing"
        
    p_res = requests.get(p_url)
    p_data = p_res.json()

    s_res = requests.get(s_url)
    s_data = s_res.json()

    for game in p_data["gameLog"]:

      season = next((s_data["seasonTotal"] for season in s_data["seasonTotals"] if season["season"] == f"{year}{year+1}"), None)
      row = {
          "TOI": game['toi'],
          "TeamRating": h_data['goalsFor'] / (h_data['goalsFor'] + h_data['goalsAgainst']),
          "OpponentRating": h_data['goalsAgainst'],
          "xTOI": season["toi"],
          "xShots": player[],
          "xShotPercentage": season[''],
          "xShotAttempts": h_data['goalsAgainst'] / h_data['shotsOnGoalAgainst'],
      }


  return

def get_all_games(year_start, year_end):

    all_games = []
    for year in range(year_start, year_end):
        if year == 2019:
            continue
        game_count = 1
        while True:
            if game_count % 100 == 0:
                print(f"Processed {game_count} games")

            game_id = f"{year}02{game_count:04}"
            url = f'https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play'
            res = requests.get(url)


            if res.status_code == 404:
                    print("Error 404: Not Found")
                    break

            data = res.json()
            res = process_game(data)
            all_games.append(res)
            game_count += 1
        max_timesteps = max(len(game) for game in all_games)
        all_games_padded = pad_sequences(all_games, maxlen=max_timesteps, padding="post", dtype=np.float32)
        np.save(f"{year}_NHL_LSTM_Game_Data.npy", all_games_padded)

    max_timesteps = max(len(game) for game in all_games)
    all_games_padded = pad_sequences(all_games, maxlen=max_timesteps, padding="post", dtype=np.float32)
    np.save("All_NHL_LSTM_Game_Data.npy", all_games_padded)

    return all_games_padded

def attention_layer(lstm_output):
    """
    Attention mechanism for LSTM outputs.
    This layer assigns attention weights to each timestep and computes a weighted sum.
    """
    # Compute attention scores
    attention_scores = Dense(1, activation='tanh')(lstm_output)  # Shape: (batch_size, timesteps, 1)
    attention_scores = Softmax(axis=1)(attention_scores)  # Normalize across timesteps

    # Apply attention weights
    context_vector = Multiply()([lstm_output, attention_scores])  # Element-wise multiply
    context_vector = Lambda(lambda x: tf.reduce_sum(x, axis=1))(context_vector)  # Corrected Keras-compatible summation

    return context_vector

def create_simple_lstm(input_dim, sequence_length, hidden_dim=128):
    model = Sequential([
        Masking(mask_value=0.0, input_shape=(sequence_length, input_dim)),  # Ignore padding

        # Core LSTM Layer
        LSTM(hidden_dim, return_sequences=False, dropout=0.3, recurrent_dropout=0.2),

        Dense(32, activation='relu'),
        Dropout(0.3),

        Dense(16, activation='relu'),

        # Output Layer (Win Probability)
        Dense(1, activation='sigmoid')
    ])

    model.compile(loss='binary_crossentropy', optimizer=tf.keras.optimizers.Adam(learning_rate=0.0005), metrics=['accuracy'])
    return model

def main():
    '''
    all_games = get_all_games(2014, 2019)
    max_timesteps = max(len(game) for game in all_games)

    X = np.array(all_games)
    y = np.array([1 if game[-1, 2] > game[-1, 3] else 0 for game in all_games])
    '''
    X = np.load("All_NHL_LSTM_Game_Data.npy")
    all_games = X.tolist()
    max_timesteps = max(len(game) for game in all_games)
    y = np.array([
        1 if len(game) > 0 and np.max(np.array(game)[:, 2]) > np.max(np.array(game)[:, 3]) else 0
        for game in all_games
    ])


    #y = np.load("nhl_labels.npy")
    #np.set_printoptions(threshold=np.inf)
    #print(X[0,:,:])
    num_games, num_timesteps, num_features = X.shape
    X_reshaped = X.reshape(-1, num_features)
    #model = tf.keras.models.load_model("nhl_lstm_win_prob.keras")
    #scaler = joblib.load("LSTM_scaler.pkl")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X_reshaped)
    X_scaled = X_scaled.reshape(num_games, num_timesteps, num_features)

    X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42, shuffle=True)

    class_weights = compute_class_weight(class_weight="balanced", classes=np.unique(y_train), y=y_train)
    class_weight_dict = {i: class_weights[i] for i in range(len(class_weights))}

    model = create_simple_lstm(input_dim=X_train.shape[2], sequence_length=X_train.shape[1])
    model.summary()

    model.fit(X_train, y_train, epochs=50, batch_size=64, validation_data=(X_test, y_test),
            class_weight=class_weight_dict,
            callbacks=[tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5, restore_best_weights=True)])

    # Evaluate
    test_loss, test_acc = model.evaluate(X_test, y_test)
    print(f"Test Accuracy: {test_acc:.2%}")

    # Save Model
    model.save("simple_nhl_lstm.keras")

    # Save the trained model
    model.save("nhl_lstm_win_prob.keras")
    joblib.dump(scaler, 'LSTM_scaler.pkl')


    test_loss, test_acc = model.evaluate(X_test, y_test)
    train_games = set(map(tuple, X_train.reshape(X_train.shape[0], -1)))
    test_games = set(map(tuple, X_test.reshape(X_test.shape[0], -1)))

    print(f"Home Wins (1s): {np.sum(y_train)}")
    print(f"Away Wins (0s): {len(y_train) - np.sum(y_train)}")

    print(f"Test Accuracy: {test_acc:.2%}")

    # Make Predictions
    y_pred_prob = model.predict(X_test)
    y_pred = (y_pred_prob > 0.5).astype(int)

    # Compare Actual vs Predicted
    from sklearn.metrics import accuracy_score
    final_acc = accuracy_score(y_test, y_pred)
    print(f"Test Set Accuracy: {final_acc:.2%}")

if __name__ == "__main__":
    main()
