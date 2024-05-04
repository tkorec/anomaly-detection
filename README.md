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
Aggregated data for monitoring from asc-clickstream-emr-output/trendlines/p2p/agg/data_version=dataset576_V11/ for P2P 
and asc-clickstream-emr-output/trendlines/pf/agg/data_version=dataset576_V11/ for PF is loaded and stored in Spark DataFrame
Cubed data for monitoring from asc-clickstream-emr-output/trendlines/p2p/cube/data_version=dataset576_V11/ for P2P
and asc-clickstream-emr-output/trendlines/pf/cube/data_version=dataset576_V11/ for PF is loaded and stored in Spyrk DataFrame
Result data 

Metrics file/class


Model file/class:
Model file/class contains all statistical tests and methods for pre-processing time series before their modeling as well as models
and modeling algorithms themselves. The main method called from metrics functions is model method that calls statistical tests
and required pre-processing fuctions based on the result of the statistical tests. 
