import json
import requests
import pandas as pd
from tqdm import tqdm
import datetime
from numpy.polynomial import Polynomial


coin = "BTC"
contract_date="24JUN22"
option = coin + "-" + contract_date


def get_option_name_and_strike(coin):
    # requests public API
    r = requests.get("https://test.deribit.com/api/v2/public/get_instruments?currency=" + coin + "&kind=option")
    result = json.loads(r.text)

    # get option name
    name = pd.json_normalize(result['result'])['instrument_name']
    name = list(name)

    # get option strike
    strike = pd.json_normalize(result['result'])['strike']
    strike = list(strike)

    # get option kind
    option_type = pd.json_normalize(result['result'])['option_type']
    option_type = list(option_type)

    return name, strike, option_type


def get_option_data(coin):
    # get option name, strike and option_type
    coin_name = get_option_name_and_strike(coin)[0]
    strike = get_option_name_and_strike(coin)[1]
    option_type = get_option_name_and_strike(coin)[2]

    # initialize data frame
    coin_df = []

    # initialize progress bar
    pbar = tqdm(total=len(coin_name))

    # loop to download data for each Option Name
    for i in range(len(coin_name)):
        if option in coin_name[i]:
            # download option data -- requests and convert json to pandas
            r = requests.get('https://test.deribit.com/api/v2/public/get_order_book?instrument_name=' + coin_name[i])
            result = json.loads(r.text)
            df = pd.json_normalize(result['result'])

            # add strike and option_type
            df['strike'] = strike[i]
            df['option_type'] = option_type[i]

            # append data to data frame
            coin_df.append(df)

        else:
            pass

        # update progress bar
        pbar.update(1)

    # finalize data frame
    coin_df = pd.concat(coin_df)
    coin_df = coin_df[['instrument_name', 'timestamp', 'option_type', 'strike', 'mark_iv']]

    # close the progress bar
    pbar.close()

    return coin_df


def get_equation(df):
    #  initialize equation dataframe
    header = ["time", "option_type", "equation", "constant", "x_coef", "x2_coef"]

    # make arraies of strike andmark_iv
    strike_arr = df[:6]['strike'].array
    mark_iv_arr = df[:6]['mark_iv'].array

    # get quadratic equation
    equation_coef = Polynomial.fit(strike_arr, mark_iv_arr, 2).coef
    constant = round(equation_coef[0], 2)
    x_coef = round(equation_coef[1], 2)
    x2_coef = round(equation_coef[2], 2)
    equation = f"{constant}+{x_coef}X+{x2_coef}X**2"

    # put data into list
    eq_list = [[datetime.datetime.now().strftime("%d/%m/%Y %H:%M"),
                df["option_type"].iloc[0],
                equation, constant, x_coef, x2_coef]]

    # transform data list into dataframe
    eq_df = pd.DataFrame(data=eq_list, columns=header)

    return eq_df

#demonstrate the data auto-refresh here, as we usually automate the script through terminal or air-flow
import time
count = 0
while count < 3:
    print(count)
    # print data and time for log
    print('Date and time: ' +  datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S") + ' , format: dd/mm/yyyy hh:mm:ss')

    # download data -- BTC a Options
    btc_option_data   = get_option_data('BTC')
    btc_option_data_c = btc_option_data[btc_option_data["option_type"] == "call"]
    btc_option_data_p = btc_option_data[btc_option_data["option_type"] == "put"]

    # transform data into equation
    equation_call = get_equation(btc_option_data_c)
    equation_put  = get_equation(btc_option_data_p)

    # export data to .csv -- append to existing
    btc_option_data.to_csv('export_csv/btc_option_data.csv', index=0, mode='a',header=False)

    # export equation to .csv -- append to existing
    equation_call.to_csv('export_csv/btc_option_equation_call.csv', index=0, mode='a',header=False)
    equation_put.to_csv('export_csv/btc_option_equation_put.csv', index=0, mode='a',header=False)
    time.sleep(1)
    count+=1