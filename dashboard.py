from flask import Flask, render_template, request
import pandas as pd
from utils import *

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('main_page.html')

@app.route('/product')
def search_one_product():
    tour_title = request.args.get('tour_title') # пока считаю что tour_title это id
    product_codes_df = get_product_codes_all()
    product_id = product_codes_df[product_codes_df['product_name'] == tour_title]['product_id'].values[0]
    true_data_df = get_true_data_all() # истинные данные
    result, errors = get_result_one_product(int(product_id), true_data_df, product_codes_df)
    return render_template('one_product_page.html', tour_title=tour_title, data=result, columns_name=list(result.columns)[3:])

@app.route('/products_date_only')
def search_date_only():
    true_data_df = get_true_data_all() # истинные данные
    product_codes_df = get_product_codes_all()
    product_codes_df = product_codes_df[product_codes_df['marketplace'].isin(['Viator', 'Sputnik8', 'Musement'])]
    product_id_all = [int(i) for i in true_data_df[true_data_df['booking_type'] == 'DATE']['product_id'].unique() if i in product_codes_df['product_id'].unique()]
    results_all_list, errors_all_list = get_result_all(product_id_all, true_data_df, product_codes_df)
    pd.concat(results_all_list).to_csv('final_res_dateonly.csv')
    return render_template('many_products_page.html', all_data = zip(results_all_list, [list(res.columns)[3:] for res in results_all_list], product_id_all), errors=errors_all_list)


@app.route('/products_timeslot')
def search_timeslot():
    true_data_df = get_true_data_all() # истинные данные
    product_codes_df = get_product_codes_all()
    product_codes_df = product_codes_df[product_codes_df['marketplace'].isin(['Viator', 'Sputnik8', 'Musement'])]
    product_id_all = [int(i) for i in true_data_df[true_data_df['booking_type'] == 'DATE_AND_TIME']['product_id'].unique() if i in product_codes_df['product_id'].unique()]
    results_all_list, errors_all_list = get_result_all(product_id_all, true_data_df, product_codes_df)
    pd.concat(results_all_list).to_csv('final_res_timeslot.csv')
    return render_template('many_products_page.html', all_data = zip(results_all_list, [list(res.columns)[3:] for res in results_all_list], product_id_all), errors=errors_all_list)


@app.route('/products_all')
def search_all():
    true_data_df = get_true_data_all()
    product_codes_df = get_product_codes_all()
    product_codes_df = product_codes_df[product_codes_df['marketplace'].isin(['Viator', 'Sputnik8', 'Musement'])]
    product_id_all = [int(i) for i in true_data_df['product_id'].unique() if i in product_codes_df['product_id'].unique()]
    results_all_list, errors_all_list = get_result_all(product_id_all, true_data_df, product_codes_df)
    pd.concat(results_all_list).to_csv('final_res.csv')
    return render_template('many_products_page.html', all_data = zip(results_all_list, [list(res.columns)[3:] for res in results_all_list], product_id_all), errors=errors_all_list)

if __name__ == "__main__":
    from gevent import monkey
    monkey.patch_all()
    app.run(host='0.0.0.0', port=5004)
