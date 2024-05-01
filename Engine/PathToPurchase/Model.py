from scipy.stats import shapiro
from scipy.stats import boxcox
import pandas as pd
from datetime import datetime
from statsmodels.tsa.stattools import adfuller
from pmdarima.arima import auto_arima


class ModelARIMA:

    def __init__(self) -> None:
        pass

    # If data isn't normally distributed, it can be transformed via BoxCox transformation, so data is normally distributed
    def boxcox_transformation(data):
        boxcox_transformed_data, _ = boxcox(data.iloc[:, 0] + 10)
        boxcox_transformed_data = pd.Series(
            boxcox_transformed_data, index=data.index
        )
        return boxcox_transformed_data

    # Distribution of data is tested via Shapiro-Wilk test
    def shapiro_wilk_test(data):
        _, p_value = shapiro(data)
        alpha = 0.05  # significance level
        if p_value > alpha:
            return True
        else:
            return False
        
    # Data's stationarity is tested via Augmented Dickey-Fuller test
    def check_stationarity(data):
        result = adfuller(data)
        if (result[1] <= 0.05) & (result[4]['5%'] > result[0]):
            is_stationary = True
        else:
            is_stationary = False
    
        return is_stationary

    # Hyndman & Khandakar algorithm called when we want to retrain models for monitored time series
    # The outputs, except for forecast and confidence interval for the day, are parameters for models
    def hyndman_khandakar(series, current_day):
        auto_model = auto_arima(series)
        forecast, conf_int = auto_model.predict(n_periods=len(current_day), return_conf_int=True, alpha=0.05)
        return forecast, conf_int

    # If forecasted value lies within the confidence interval, we observe an expected value that isn't an outlier, 
    # and verdict of the monitoring is therefore OK.
    # If forecasted value lies outside the confidence interval, we observe an outlier values and verdict of the
    # monitoring is therefore NOK
    def confidential_boundaries_verdict(forecast, conf_int):
        upper_bound = conf_int[:,1]
        lower_bound = conf_int[:,0]
        if lower_bound < forecast < upper_bound:
            verdict = "OK"
        else:
            verdict = "NOK"
        return verdict
    
    def arima_model(series, p, d, q):

        return forecast, conf_int, p, d, q


    def model(self, data, attributes):
        data = data.toPandas().reset_index(drop=True).set_index("date", inplace=True)

        # ARIMA model needs data that is normally distributed
        # If it's not normally distributed, BoxCox transformation may be applied
        is_data_gaussian = self.shapiro_wilk_test(data)
        if is_data_gaussian == False:
            data = self.boxcox_transformation(data)

        # ARIMA model needs data to be stationary
        # If data isn't stationary, we have to differentiate it until it is
        is_data_stationary = self.check_stationarity(data)
        while is_data_stationary == False:
            data = data.diff()
            data = data.dropna()
            is_data_stationary = self.check_stationarity(data)

        # Attributes for calling ARIMA model functions: data and model's parameters
        p, d, q = attributes[0], attributes[1], attributes[2]
        historic_data = data[:-1]
        current_day = data[-1:]
        series = pd.Series(historic_data, index=pd.date_range(historic_data.index[0], periods=len(historic_data), freq="D"), name="Series")
    

        # So the modeling/predicting runs faster, we use ARIMA model found by Hyndman & Khandakar auto ARIMA algorithm for 2 months.
        # On the 1st of every second month, after completely new data exist, the models for time series are retrained
        if (datetime.now().month % 2) == 1 and datetime.now().day == 1:
            forecast, conf_int, p, d, q = self.hyndman_khandakar(series)
        else:
            forecast, conf_int, p, d, q = self.arima_model(series, p, d, q)
        
        # Except for the expected value based on model's prediction, 
        # ARIMA model returned us also confidence interval – standard deviation from the expected/forecasted value.
        # Verdict is made based on whether the expected value lies within or outside confident interval
        verdict = self.confidential_boundaries_verdict(forecast, conf_int)

        return current_day, forecast, verdict, p, d, q

        



