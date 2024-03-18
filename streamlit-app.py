import psycopg2
import streamlit as st
import time
from kafka import KafkaConsumer
import simplejson as json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


@st.cache_data
def fetch_voting_stats():
    conn = psycopg2.connect('host=localhost dbname=voting user=postgres password=postgres')
    cur = conn.cursor()

    # Fetch total number of voters
    cur.execute('select count(*) voters_count from voters')
    voters_count = cur.fetchone()[0]

    # Fetch total number of candidates
    cur.execute('select count(*) candidates_count from candidates')
    candidates_count = cur.fetchone()[0]

    return voters_count, candidates_count


def create_kafka_consumer(topic_name):
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    return consumer


def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
            print(sub_message.value)
    return data



def plot_colored_bar_chart(results):
    data_type = results['candidate_name']
    colors = plt.cm.viridis(np.linspace(0, 1, len(data_type)))

    bars = plt.bar(data_type, results['total_votes'], color=colors)
    
    plt.bar(data_type, results['total_votes'], color=colors)
    plt.xlabel('Candidate')
    plt.ylabel('Total Votes')
    plt.title('Vote Count by Candidates')
    plt.xticks(rotation=90)

    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, round(yval, 2), va='bottom')

    return plt


def plot_donut_chart(data):
    labels = list(data['candidate_name'])
    sizes = list(data['total_votes'])

    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle = 140)
    ax.axis('equal')
    plt.title('Candidates Votes')
    
    return fig



def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Fetch voting stats from postgres
    voters_count, candidates_count = fetch_voting_stats()

    # Display statistics
    st.markdown('---')
    col1, col2 = st.columns(2)
    col1.metric('Total Voters', voters_count)
    col2.metric('Total Candidates', candidates_count)

    topic_name = 'aggregated_votes_per_candidate'
    consumer = create_kafka_consumer(topic_name)
    data = fetch_data_from_kafka(consumer)

    results = pd.DataFrame(data)

    # Identify leading candidate
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]
    leading_candidate = results.loc[results['total_votes'].idxmax()]
    
    # Display the leading candidate information
    st.markdown('---')
    st.header('Leading Candidate')
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader('Total Votes: {}'.format(leading_candidate['total_votes']))


    # Display the statistics and visualisations
    st.markdown('---')
    st.header('Voting Statistics')
    result = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)

    # Display the bar chart and donut chart
    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)

    with col2:
        donut_fig = plot_donut_chart(results)
        st.pyplot(donut_fig)


st.title('Realtime Election Voting Dashboard')


if __name__ == '__main__':
    update_data()