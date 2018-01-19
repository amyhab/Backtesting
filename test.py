# -*- coding: utf-8 -*-
"""
Created on Thu Jan 18 08:56:11 2018

@author: bryan.tu
"""
import pandas as pd 
from back_test import Backtest

alg = Backtest(index='000905',read_db_if=1,portfolio_num=60,hedge_target='000905',factor_order=1,trade_cost=0.0012,
                 industry_control=2,weight_low_bank=0.0,weight_low_nonbank=0.0,weight_low_realestate=0.0,
                 factor_standardize=0,industry_neutral=0,portfolio_weight='Mosek',mosek_high_bound=0.02,ic_day=20)
alg.backtest(startday='2007-01-15',endday = '2018-01-12',base_indus_day='2017-01-04')
alg.analyze(startday='2007-01-15')