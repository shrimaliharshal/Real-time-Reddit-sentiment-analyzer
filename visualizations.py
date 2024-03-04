from matplotlib import pyplot as plt
import pandas as pd
import numpy as np
from matplotlib.colors import ListedColormap
from matplotlib.collections import LineCollection
import seaborn as sns
from fastf1.ergast import Ergast
import plotly.express as px
from plotly.io import show
from matplotlib import cm
import pandas as pd
import streamlit as st


df = pd.read_csv("D:\MSDA 1st sem\Spring 24\BIG DATA - DATA 228\HW2\Sentiment_Analysis_Report.csv")
st.title('Reddit Scraper')
def plot1(df):
    plt.figure(figsize=(10, 6))
    sns.countplot(x='keyword', hue='sentiment1', data = df)
    plt.title('Sentiment Distribution per Keyword')
    plt.xticks(rotation=45)
    plt.tight_layout()  
    return plt.gcf()



def plot2(pandas_test):
    pandas_test['Sentiment Agreement'] = ['Agree' if x == y else 'Disagree' for x, y in zip(pandas_test['sentiment1'], pandas_test['sentiment2'])]
    # Now, create the plot
    plt.figure(figsize=(8, 6))
    ax = sns.countplot(x='Sentiment Agreement', data=pandas_test, palette='Set2')
    plt.title('Comparison of Sentiment Analysis Agreement')
    plt.xlabel('Sentiment Agreement Between Two Analyses')
    plt.ylabel('Number of Posts')

    # Annotate counts above bars (optional)
    for p in ax.patches:
        ax.annotate(f'{int(p.get_height())}', (p.get_x() + p.get_width() / 2., p.get_height()),
                    ha='center', va='center', fontsize=10, color='black', xytext=(0, 5),
                    textcoords='offset points')

    plt.tight_layout()
    # plt.show()
    return plt.gcf()

def plot3(pandas_test):
    # Assuming 'pandas_test' is your DataFrame
    pandas_test['Sentiment Agreement'] = ['Agree' if x == y else 'Disagree' for x, y in zip(pandas_test['sentiment1'], pandas_test['sentiment2'])]
    # Unique list of keywords plus 'All' option
    keywords = ['All'] + sorted(pandas_test['keyword'].unique())

    # Create the figure
    fig = px.histogram(pandas_test, x='Sentiment Agreement', color='Sentiment Agreement',
                    title='Sentiment Analysis Agreement by Keyword',
                    labels={'Sentiment Agreement': 'Agreement Between Analyses'},
                    barmode='group')

    # Create a button for each keyword, including 'All'
    buttons = []

    for key in keywords:
        if key == 'All':
            # Filter for all data
            df_filtered = pandas_test
        else:
            # Filter data for the selected keyword
            df_filtered = pandas_test[pandas_test['keyword'] == key]

        # Count the occurrences for the filtered DataFrame
        counts = df_filtered['Sentiment Agreement'].value_counts().reset_index()
        counts.columns = ['Sentiment Agreement', 'count']

        buttons.append(dict(method='restyle',
                            label=key,
                            visible=True,
                            args=[{'y': [counts['count']],
                                'x': [counts['Sentiment Agreement']],
                                'type': 'bar'}, [0]],
                            )
                    )

    # Update layout with dropdown
    fig.update_layout(
        updatemenus=[dict(buttons=buttons,
                        direction='down',
                        pad={'r': 10, 't': 10},
                        showactive=True,
                        x=0.1,
                        xanchor='left',
                        y=1.15,
                        yanchor='top')]
    )

    # Update axes titles
    fig.update_xaxes(title_text='Sentiment Agreement')
    fig.update_yaxes(title_text='Number of Posts')

    # fig.show()
    return fig


with st.sidebar:
    st.title('Visualization')
    


# Visualization w. custom input
if st.button(' Emotions in posts'):
    fig_tyre = plot1(df)
    st.pyplot(fig_tyre)

if st.button('Agreement between Sentiment Analysis'):
    fig_lapscatter = plot2(df)
    st.pyplot(fig_lapscatter)

if st.button('Sentiment Analysis Agreement by Keyword'):
    fig_results =plot3(df)
    st.plotly_chart(fig_results)