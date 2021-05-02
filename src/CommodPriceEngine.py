import os
import pandas as pd
from pandas import melt
import numpy as np
import calendar
from datetime import datetime


class CommodPriceEngine(object):
    def __init__(self, **params):
        params = params.get('params')
        self.commod_ids = params.get('commod_ids')
        self.input_dir = params.get('input_dir')
        self.output_dir = params.get('output_dir')
        self.raw_prices_dir = os.path.join(self.output_dir, "raw_prices")
        self.transform_dir = os.path.join(self.output_dir, "transform")
        self.transform_file = os.path.join(self.transform_dir, "full_transform.csv")
        self.matrix_price_dir = os.path.join(self.output_dir, "matrix_price")

    def extract_raw_prices(self, sheet_names=None):
        data_folder = self.input_dir
        sheet_names = {
            "2tab": ("new_format", [2, 3]),
            " Prices US": ("old_format", [2]),
            "All Prices": ("old_format", [2])
        } if sheet_names is None else sheet_names
        eia_files = next(os.walk(data_folder))[2]
        for eia_file in eia_files:
            if ("~" == eia_file[0]) or ("xls" not in eia_file.split(".")[1].lower()):
                continue
            xl = pd.ExcelFile(os.path.join(data_folder, eia_file))
            sheet_name = None
            for s_name in sheet_names.keys():
                if s_name in xl.sheet_names:
                    sheet_name = s_name
                    break
            if sheet_name is None:
                continue
            format_type, header_info = sheet_names[sheet_name]
            df = xl.parse(sheet_name, header=header_info)
            ref_date = eia_file.split(".")[0]
            ref_date = ref_date.split("_")[0]
            mth = ref_date[0:3]
            # assuming all input reports will be in this century
            yr = "20" + ref_date[3:]
            df[("ref_date", "0")] = f"{yr}-{mth}"
            df[("ref_date", "0")] = pd.to_datetime(df[("ref_date", "0")])
            df.to_csv(os.path.join(self.raw_prices_dir,
                                   "{}-{}.csv".format(ref_date, format_type)),
                      index=False)

    @staticmethod
    def cnvt_date_fmt(inp_date):
        res = datetime.strptime(inp_date, "%Y%m").strftime("%Y-%m")
        yr = res.split("-")[0]
        cal_dic_idx = {
            index: month for index, month in enumerate(
                calendar.month_abbr
            ) if month
        }
        mn = cal_dic_idx[int(res.split("-")[1])]
        return f"{yr}-{mn}"

    def transform_prices(self):
        ret = []
        raw_folder = self.raw_prices_dir
        raw_files = next(os.walk(raw_folder))[2]
        for raw_file in raw_files:
            if "csv" not in raw_file.split(".")[1].lower():
                continue
            old_format = "old_format" in raw_file
            header = [0] if old_format else [0, 1]
            df = pd.read_csv(os.path.join(raw_folder, raw_file), header=header)
            df = df[df[df.columns[0]].isin(self.commod_ids)]
            ref_date_idx = "('ref_date', '0')" if old_format else ("ref_date", "0")
            df.set_index([df.columns[0], ref_date_idx], inplace=True)
            df.drop(list(df.columns[0:1]), inplace=True, axis=1)
            df = df.T
            df["date"] = df.index
            date_cnvt_func = self.cnvt_date_fmt if old_format else lambda x: f"{x[0]}-{x[1]}"
            df["date"] = df["date"].apply(date_cnvt_func)
            res = melt(df, id_vars=["date"])
            res.columns = ["date", "commodity_id", "ref_date", "price"]
            res["date"] = pd.to_datetime(res["date"])
            res["price"] = res["price"].astype(float)
            ret.append(res)
        tdf = pd.concat(ret)
        tdf.reset_index(inplace=True, drop=True)
        tdf.to_csv(
            self.transform_file,
            index=False
        )

    def build_single_price_matrix(self, commod_id: str):
        tdf = pd.read_csv(self.transform_file)
        prev_df = tdf[tdf["commodity_id"] == commod_id].copy().sort_values(
            by=["ref_date", "date"], ascending=True
        )
        ret = []
        for in_ref_date in sorted(tdf["ref_date"].unique()):
            in_df = prev_df[prev_df["ref_date"] == in_ref_date].copy()
            in_df["pct_diff"] = in_df["price"].pct_change()
            ret.append(in_df)
        df = pd.concat(ret).sort_values(by=["ref_date", "date"], ascending=True)
        df.drop(columns=["commodity_id"], inplace=True)
        piv_df = df.pivot_table(
            index=['ref_date'],
            columns=['date'],
            values='price'
        )
        piv_df.to_csv(
            os.path.join(self.matrix_price_dir, "{}_price.csv".format(commod_id))
        )
        piv_df = df.pivot_table(
            index=['ref_date'],
            columns=['date'],
            values='pct_diff'
        )
        piv_df.to_csv(
            os.path.join(self.matrix_price_dir, "{}_pct.csv".format(commod_id))
        )

    def build_price_matrix(self):
        for commod_id in self.commod_ids:
            self.build_single_price_matrix(commod_id)

    def main(self, acquire=True):
        if acquire:
            self.extract_raw_prices()
        self.transform_prices()
        self.build_price_matrix()
