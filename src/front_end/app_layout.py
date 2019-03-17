import pandas as pd
import dash_html_components as html
import dash_core_components as dcc
import secret
import psycopg2


conn = psycopg2.connect(
    host=secret.host, database=secret.dbname,
    user=secret.user, password=secret.password)

df4 = pd.read_sql_query(
    'select * from "gharchive" order by date_created_at_1', con=conn)

df_wow = pd.read_sql_query(
    'select * from "gharchivewowtest" order by week_created_at_1', con=conn)
df_mom = pd.read_sql_query(
    'select * from "gharchivemomtest" order by month_created_at_1', con=conn)
recent = pd.read_sql_query(
    'select count_1 from "gharchive" order by date_created_at_1 desc limit 1',
    con=conn).count_1[0]

text = df4.weekly_increase_rate.round(3).apply(lambda x: str(x*100) + '%')
text = df_mom.monthly_increase_rate.round(3).apply(lambda x: str(x*100) + '%')

app_alyout = html.Div([
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
    dcc.Interval(
        id='interval-component',
        interval=60*1000,
        n_intervals=0)
    ]
)