from scipy.stats import shapiro
from scipy.stats import boxcox
import pandas as pd
from statsmodels.tsa.stattools import adfuller
from pmdarima.arima import auto_arima


class ModelARIMA:

    def __init__(self) -> None:
        pass

    def boxcox_transformation(data):
        boxcox_transformed_data, _ = boxcox(data.iloc[:, 0] + 10)
        boxcox_transformed_data = pd.Series(
            boxcox_transformed_data, index=data.index
        )
        return boxcox_transformed_data

    
    def shapiro_wilk_test(data):
        _, p_value = shapiro(data)
        alpha = 0.05  # significance level
        if p_value > alpha:
            return True
        else:
            return False
        
    def check_stationarity(data):
        result = adfuller(data)
        if (result[1] <= 0.05) & (result[4]['5%'] > result[0]):
            is_stationary = True
        else:
            is_stationary = False
    
        return is_stationary

    def hyndman_khandakar(series, current_day):
        auto_model = auto_arima(series)
        forecast, conf_int = auto_model.predict(n_periods=len(current_day), return_conf_int=True, alpha=0.05)
        return forecast, conf_int

    def confidential_boundaries_verdict(forecast, conf_int):
        upper_bound = conf_int[:,1]
        lower_bound = conf_int[:,0]
        if lower_bound < forecast < upper_bound:
            verdict = "OK"
        else:
            verdict = "NOK"
        return verdict

    def model(data):
        data = data.toPandas().reset_index(drop=True).set_index("date", inplace=True)

        is_data_gaussian = shapiro_wilk_test(data)
        while is_data_gaussian == False:
            data = boxcox_transformation(data)
            is_data_gaussian = shapiro_wilk_test(data)

        is_data_stationary = check_stationarity(data)
        while is_data_stationary == False:
            data = data.diff()
            data = data.dropna()
            is_data_stationary = check_stationarity(data)

        historic_data = data[:-1]
        current_day = data[-1:]

        series = pd.Series(historic_data, index=pd.date_range(historic_data.index[0], periods=len(historic_data), freq="D"), name="Series")

        forecast, conf_int = hyndman_khandakar(series, current_day)

        verdict = confidential_boundaries_verdict(forecast, conf_int)

        



