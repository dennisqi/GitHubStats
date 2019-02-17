import dash
import random
import psycopg2
from dash.dependencies import Input, Output
import secret
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
from datetime import date
import calendar
import flask
import pandas as pd
import time
import os

# # configure of DB
# host = os.environ.get('PG_HOST')
# dbname = 'dbname'
# user = os.environ.get('PG_USER')
# password = os.environ.get('PG_PSWD')
# print(host, dbname, user, password)
conn = psycopg2.connect(
    host=secret.host, database=secret.dbname,
    user=secret.user, password=secret.password)

# df0 = pd.read_sql_query(
#     'select * from "gharchive" order by date_created_at_1', con=conn)
# df1 = pd.read_sql_query(
#     'select * from "gharchive" where weekly_increase_rate > 0', con=conn)
# df2 = pd.read_sql_query(
#     'select * from "gharchive" where weekly_increase_rate < 0', con=conn)
# df3 = pd.read_sql_query(
#     'select * from "gharchive" where weekly_increase_rate = 0', con=conn)
df4 = pd.read_sql_query(
    'select * from "gharchive" order by date_created_at_1', con=conn)

df_wow = pd.read_sql_query(
    'select * from "gharchivewowtest" order by week_created_at_1', con=conn)
df_mom = pd.read_sql_query(
    'select * from "gharchivemomtest" order by month_created_at_1', con=conn)
recent = pd.read_sql_query(
    'select count_1 from "gharchive" order by date_created_at_1 desc limit 1',
    con=conn).count_1[0]

server = flask.Flask('app')
server.secret_key = os.environ.get('secret_key', 'secret')

df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/hello-world-stock.csv')

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash('app', server=server, external_stylesheets=external_stylesheets)

text = df4.weekly_increase_rate.round(3).apply(lambda x: str(x*100) + '%')
text = df_mom.monthly_increase_rate.round(3).apply(lambda x: str(x*100) + '%')
# text = [
#     text[i]
#     + '\n'
#     + calendar.day_name[df4.date_created_at_1[i].weekday()]
#     for i in range(len(text))]

app.index_string = '''
<!DOCTYPE html>
<html>
    <head>
        <title>Repo Census</title>
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.2.1/css/bootstrap.min.css" integrity="sha384-GJzZqFGwb1QTTN6wy59ffF1BuGJpLSa9DkKMp0DgiMDm4iYMj70gZWKYbI706tWS" crossorigin="anonymous">
        {%css%}
    </head>
    <body>
        <header>
            <div class="collapse bg-dark" id="navbarHeader">
                <div class="container">
                    <div class="row">
                        <div class="col-sm-8 col-md-7 py-4">
                            <h4 class="text-white">About</h4>
                            <p class="text-muted">Add some information about the album below, the author, or any other background context. Make it a few sentences long so folks can pick up some informative tidbits. Then, link them off to some social
                                networking sites or contact information.</p>
                        </div>
                        <div class="col-sm-4 offset-md-1 py-4">
                            <h4 class="text-white">Contact</h4>
                            <ul class="list-unstyled">
                                <li><a href="#" class="text-white">Follow on Twitter</a></li>
                                <li><a href="#" class="text-white">Like on Facebook</a></li>
                                <li><a href="#" class="text-white">Email me</a></li>
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
            <div class="navbar navbar-dark bg-dark shadow-sm">
                <div class="container d-flex justify-content-between">
                    <a href="#" class="navbar-brand d-flex align-items-center" style="text-decoration: none">
                        <img src="https://upload.wikimedia.org/wikipedia/commons/thumb/1/1d/Line_chart_icon_Noun_70892_cc_White.svg/2000px-Line_chart_icon_Noun_70892_cc_White.svg.png" alt="" width="20" height="20" class="mr-1">
                        <strong>GGM</strong>
                    </a>
                </div>
            </div>
        </header>
        <div class="container">
            <section class="jumbotron text-center mt-5">
                <div class="container">
                    <p>
                        GitHub is one of the largest open source community in the world. Repo Census aims to measure the history and creation of GitHub repositories
                    </p>
                </div>
            </section>
            {%app_entry%}
            <footer class="text-muted" style="padding-top:3rem; padding-bottom:3rem">
                {%config%}
                {%scripts%}
                <div class="container">
                    <p class="float-right">
                        <a href="#">Back to top</a>
                    </p>
                    Dataset from <a href="www.gharchive.org">gharchive</a>
                </div>
            </footer>
        </div>
    </body>
</html>
'''

app.layout = html.Div([
    html.Ul(
        [
            html.Li(
                [html.H1([
                    html.Mark(
                        'Repo Census',
                        style={
                            'display': 'inline-block',
                            'line-height': '0em',
                            'padding-bottom': '0.5em'
                        }
                    )]
                )],
                style={'display': 'inline-block', 'float': 'left'}),
            html.Li([
                html.Div([
                    html.Div([
                        html.Center(
                            [
                                html.P(
                                    'YESTERDAY ('
                                    + str(df4['date_created_at_1'][len(df4['date_created_at_1'])-1])
                                    + '), THERE WERE',
                                    style={
                                        'font-weight': 'bold',
                                        'font-size': '1.3em',
                                        'color': '#aaa',
                                        'text-align': 'center',
                                        'margin': '5px 5px',
                                    }
                                ),
                                html.P(
                                    recent,
                                    style={
                                        'font-weight': 'bold',
                                        'font-size': '2.8em',
                                        'color': '#0092bf',
                                        'margin': '0 5px',
                                    },
                                    id='latest_num_of_repo_ce'
                                ),
                                html.P(
                                    'REPOSITORIES CREATED',
                                    style={
                                        'font-weight': 'bold',
                                        'font-size': '1.3em',
                                        'color': '#aaa',
                                        'text-align': 'center',
                                        'margin': '5px 5px',
                                    }
                                )
                            ]
                        )
                    ])
                    ],
                    style={
                        'box-shadow': '0 4px 8px 0 rgba(0,0,0,0.2)',
                        'width': 'auto',
                        'height': 'auto',
                        'margin': '10px'})
                ],
                style={
                        'display': 'inline-block',
                        'float': 'right'
                }
            )
        ],
        style={'overflow': 'hidden', 'column-count': 2}
    ),
    dcc.Graph(
        id='my-graph',
        figure={
            'data': [
                {
                    'x': df4.date_created_at_1,
                    'y': df4.count_1,
                    'text': text,
                    'name': 'Number of Repository Creation',
                    # 'mode': 'lines+markers'
                },
            ],
            'layout': {'title': 'Number of Repository Creation'}
        }
    ),
    dcc.Graph(
        id='mom-graph',
        figure={
            'data': [
                {
                    'x': df_mom.month_created_at_1,
                    'y': df_mom.count_1,
                    'text': text,
                    'name': 'Month-over-Month Number of Repository Creation',
                    # 'mode': 'lines+markers'
                },
            ],
            'layout': {'title': 'Month-over-Month Number of Repository Creation'}
        }
    ),
    # dcc.Graph(
    #     id='wow-graph',
    #     figure={
    #         'data': [
    #             {
    #                 'x': df_wow.week_created_at_1,
    #                 'y': df_wow.count_1,
    #                 'text': text,
    #                 'name': 'Week-over-Week Number of Repository Creation',
    #                 # 'mode': 'lines+markers'
    #             },
    #         ],
    #         'layout': {'title': 'Week-over-Week Number of Repository CreationNumber of Repository Creation'}
    #     }
    # ),
    # dcc.Graph(
    #     id='my-graph-with-color',
    #     figure={
    #         'data': [
    #             {
    #                 'x': df4.date_created_at_1,
    #                 'y': df4.weekly_increase_rate,
    #                 'text': df4.weekly_increase_rate.round(3).apply(lambda x: 'Day-of-Week growth rate:' + str(x)),
    #                 'name': 'Growth Rate Compare to Last Week',
    #                 'mode': 'lines+markers'
    #             }
    #         ],
    #         'layout': {
    #             'title': 'Growth Rate Compare to Last Week'
    #             # 'legend': dict(orientation='h')
    #         }
    #     }
    # ),
    dcc.Interval(
        id='interval-component',
        interval=60*1000,
        n_intervals=0)
    ]
)


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
