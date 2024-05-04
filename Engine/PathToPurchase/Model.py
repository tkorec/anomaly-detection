from scipy.stats import shapiro
from scipy.stats import boxcox
import pandas as pd
import statsmodels.api as sm
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
            return True
        else:
            return False
    

    # Hyndman & Khandakar algorithm called when we want to retrain models for monitored time series
    # The outputs, except for forecast and confidence interval for the day, are parameters for models
    def hyndman_khandakar(series):
        auto_model = auto_arima(series)
        forecast, conf_int = auto_model.predict(n_periods=1, return_conf_int=True, alpha=0.05)
        conf_int = conf_int[0]
        order = list(auto_model.order)
        seasonal_order = list(auto_model.seasonal_order)
        parameters = []
        parameters.append(order)
        parameters.append(seasonal_order)
        return forecast, conf_int, parameters

    # If forecasted value lies within the confidence interval, we observe an expected value that isn't an outlier, 
    # and verdict of the monitoring is therefore OK.
    # If forecasted value lies outside the confidence interval, we observe an outlier values and verdict of the
    # monitoring is therefore NOK
    def confidential_boundaries_verdict(forecast, conf_int, current_day):
        upper_bound = conf_int[1]
        lower_bound = conf_int[0]
        error_difference = abs(current_day - forecast) / forecast
        if lower_bound < forecast < upper_bound:
            verdict = "OK"
        elif error_difference >= 0.5:
            verdict = "ERROR"
        elif current_day == 0:
            verdict = "MISSING"
        else:
            verdict = "NOK"
        return verdict
    

    def arima_model(series, parameters):
        order = tuple(parameters[0])
        seasonal_order = tuple(parameters[1])
        model = sm.tsa.statespace.SARIMAX(series, order=order, seasonal_order=seasonal_order, trend="c")
        res = model.fit(disp=False)
        forecast = res.get_prediction(steps=1)
        expected = forecast.predicted_mean.values[0]
        conf_int = forecast.conf_int(alpha=0.05).values.tolist()[0]
        return expected, conf_int, parameters


    def model(self, data, parameters):
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

        # Data split for todays day and data for modeling
        series = data[:-1]
        current_day = data[-1:]
    
        # So the modeling/predicting runs faster, we use ARIMA model found by Hyndman & Khandakar auto ARIMA algorithm for 2 months.
        # On the 1st of every second month, after completely new data exist, the models for time series are retrained
        if (datetime.now().month % 2) == 1 and datetime.now().day == 1:
            forecast, conf_int, parameters = self.hyndman_khandakar(series)
        else:
            forecast, conf_int, parameters = self.arima_model(series, parameters)
        
        # Except for the expected value based on model's prediction, 
        # ARIMA model returned us also confidence interval – standard deviation from the expected/forecasted value.
        # Verdict is made based on whether the expected value lies within or outside confident interval
        verdict = self.confidential_boundaries_verdict(forecast, conf_int, current_day)

        return current_day, forecast, verdict, parameters

        



