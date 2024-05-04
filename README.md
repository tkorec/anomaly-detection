Objectives:

Because of metrics quantity, their granularities (per domains, datasets, patternIds, behaviors, etc.), and number of analysed domains,
hundreds of time series must be monitored every day for making their futures values prediction and therefore recognizing potential
anomalies. It is a common task solved in Business Inteligence area for which commercial softwares depending on auto ARIMA algorithms 
are used, for example Forecast Pro by Business Forecast Systems, Inc.
This engine depends on open source auto ARIMA algorithm developed by Hyndman and Khandakar. It allows to its user to save for licence
fee usualy paid for commercial algorithms. The engine also reuses ARIMA model's parameters found by Hyndman&Khandakar algorithm for
static ARIMA model for a defined time, so the Hyndman&Khandakar algorithm doesn't have to be executed every day for model retraining
especially while circumstances reperesented by time series don't change at a fast pace. It allows decreasing computing time, potentially
saving expanses if the engine runs in AWS environment.

Structure:
Engine file/class


Data Loader file/class


Metrics file/class


Model file/class
