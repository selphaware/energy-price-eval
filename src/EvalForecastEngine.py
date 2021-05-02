import os
import pandas as pd
from itertools import product
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from sklearn.metrics import mean_absolute_error as mae
from sklearn.metrics import mean_squared_error as mse
from sklearn.metrics import max_error as max_error

eval_funcs = {
    "mae": mae,
    "mse": mse,
    "rmse": mse,
    "max_error": max_error
}


class EvalForecastEngine(object):
    def __init__(self, **params):
        params = params.get('params')
        self.commod_ids = params.get('commod_ids')
        self.output_dir = params.get('output_dir')
        self.matrix_price_dir = os.path.join(self.output_dir, "matrix_price")
        self.eval_dir = os.path.join(self.output_dir, "forecast_evals")
        self.metrics = params.get('eval_params')['metrics']
        self.time_horizons = params.get('eval_params')['time_horizons']

    def eval_forecasts(self, commod_id: str):
        ff = pd.read_csv(os.path.join(self.matrix_price_dir, f"{commod_id}.csv"),
                         parse_dates=["ref_date"],
                         date_parser=lambda x: datetime.strptime(x, '%Y-%m-%d'))
        ret = {f"{x}M_{y}": [] for x, y in product(self.time_horizons, self.metrics)}
        ret["ref_date"] = []
        for ix in range(ff.shape[0]):
            idx = ff.iloc[ix, 0]
            ref_date = \
            ff.loc[ff["ref_date"] == idx, ff.columns[0]].reset_index(drop=True)[
                0]
            ret["ref_date"].append(ref_date)
            fr = ff.loc[ff["ref_date"] == idx, ff.columns[1:]]
            fr.columns = pd.to_datetime(fr.columns)
            fr = fr.T
            for time_h, metric in product(self.time_horizons, self.metrics):
                end_date = ref_date + relativedelta(months=time_h)
                fore_dat = fr[(fr.index >= ref_date) & (fr.index < end_date)]
                fr2 = ff.loc[ff["ref_date"] == idx + relativedelta(
                    months=time_h), ff.columns[1:]]
                fr2.columns = pd.to_datetime(fr2.columns)
                fr2 = fr2.T
                true_dat = fr2[(fr2.index >= ref_date) & (fr2.index < end_date)]
                if true_dat.shape[1]:
                    if metric == "rmse":
                        ret[f"{time_h}M_{metric}"].append(
                            eval_funcs[metric](np.array(fore_dat),
                                               np.array(true_dat),
                                               squared=False))
                    else:
                        ret[f"{time_h}M_{metric}"].append(
                            eval_funcs[metric](np.array(fore_dat),
                                               np.array(true_dat)))
                else:
                    ret[f"{time_h}M_{metric}"].append(None)

        df = pd.DataFrame(ret)
        df['commod_id'] = commod_id
        df.reset_index(inplace=True, drop=True)
        df = df[["commod_id", "ref_date"] + [f"{x}M_{y}" for x, y in
                                             product(self.time_horizons,
                                                     self.metrics)]]
        df.to_csv(os.path.join(self.eval_dir, f"{commod_id}.csv"), index=False)

    def eval_all_forecasts(self):
        for commod_id in self.commod_ids:
            self.eval_forecasts(commod_id)
