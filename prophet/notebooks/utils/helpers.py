import numpy as np
from sklearn.metrics import mean_absolute_error, r2_score

# Function to calculate MAE
def calculate_mae(y_true, y_pred):
    return mean_absolute_error(y_true, y_pred)

# Function to calculate MAPE
def calculate_mape(y_true, y_pred):
    return np.mean(np.abs((np.array(y_true) - np.array(y_pred)) / np.array(y_true))) * 100

# Function to calculate RMSE
def calculate_rmse(y_true, y_pred):
    mse = np.mean((np.array(y_true) - np.array(y_pred)) ** 2)
    return np.sqrt(mse)

# Function to calculate R-squared
def calculate_r2(y_true, y_pred):
    return r2_score(y_true, y_pred)

def performance_metrics(y_true, y_pred):
    results = {'mae': calculate_mae(y_true, y_pred),
               'mape': calculate_mape(y_true, y_pred),
               'rmse': calculate_rmse(y_true, y_pred),
               'r2': calculate_r2(y_true, y_pred)}
    
    print(f"MAE: {results['mae']:.2f}")
    print(f"MAPE: {results['mape']:.2f}%")
    print(f"RMSE: {results['rmse']:.2f}")
    print(f"R2: {results['r2']:.2f}")

    return results