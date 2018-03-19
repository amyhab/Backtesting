# Backtesting
Backtesting module used for stock selection<br>
Function: backtest by stock seleced by factor from target stock pool<br>
Market Database: datayes<br>
Factor Data: must be Dataframe with index = code and columns = tradeday, saved in factor.csv<br>
Library Needed: Mosek 8.1, used for optimization <br>
<br>
For example, the backtest results of a strategy which used Xgboost on fundamental data are as follows<br>
![image1](https://github.com/amyhab/Backtesting/blob/master/Backtest1.JPG)<br>
![image2](https://github.com/amyhab/Backtesting/blob/master/Backtest2.JPG)<br>
![image3](https://github.com/amyhab/Backtesting/blob/master/Backtest3.JPG)<br>

