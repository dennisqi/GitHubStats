import dash
import random
import psycopg2
from dash.dependencies import Input, Output
import secret
import flask
import pandas as pd
import os
from html_text import html_text
from app_layout import app_layout

conn = psycopg2.connect(
    host=secret.host, database=secret.dbname,
    user=secret.user, password=secret.password)

server = flask.Flask('app')
server.secret_key = os.environ.get('secret_key', 'secret')

df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/hello-world-stock.csv')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash('app', server=server, external_stylesheets=external_stylesheets)

app.index_string = html_text

app.layout = app_layout


@app.callback(
    Output('latest_num_of_repo_ce', 'children'),
    [Input('interval-component', 'n_intervals')])
def update_metrics(n):
    recent = pd.read_sql_query(
        'select count_1 from "gharchive" order by date_created_at_1 desc limit 1',
        con=conn).count_1[0]
    recent = "{:,}".format(recent)
    # recent = random.randint(1000, 2000)
    return str(recent)


def main(a, b):
    app.run_server(host='localhost', port=8000)


if __name__ == '__main__':
    app.run_server(host='localhost', port=8000)
