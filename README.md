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


Data Loader file/class:
Aggregated data (contains data for aggregated metrics) for monitoring from asc-clickstream-emr-output/trendlines/p2p/agg/data_version=dataset576_V11/ 
for P2P and asc-clickstream-emr-output/trendlines/pf/agg/data_version=dataset576_V11/ for PF is loaded and stored in Spark DataFrame
Cubed data for monitoring from asc-clickstream-emr-output/trendlines/p2p/cube/data_version=dataset576_V11/ for P2P
and asc-clickstream-emr-output/trendlines/pf/cube/data_version=dataset576_V11/ for PF is loaded and stored in Spyrk DataFrame
Result data 

Metrics file/class


Model file/class:
Model file/class contains all statistical tests and methods for pre-processing time series before their modeling as well as models
and modeling algorithms themselves. The main method called from metrics functions is model method that calls statistical tests
and required pre-processing fuctions based on the result of the statistical tests. The ARIMA model's parameters (p, d, q) for a particular
time series, time series for a particular combination of metric, domain, dataset, behavior, patternId, etc., retrieved from Result DataFrame
is passed to modeling function (arima_model func.) that return predicted value and confidence interval. It also returns model's parameters
which are same as the ones passed to the function. On a specified day on which the retarining of the models is required, the hyndman_khandakarr
method is called instead of arima_model method. Hyndman_khandakar method performs Hyndman&Khandakar algorithms for finding new model. It then
return expected value based on time series, confidence interval, and new model's parameters that are stored to Results and used until the
next retraining of models.
