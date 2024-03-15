import psycopg2
import streamlit as st
import time

st.title('Realtime Election')
st.write('Realtime Election Voting Dashboard')

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

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f'Last refresh at: {time.strftime('%Y-%m-%d %H:%M:%S')} ')

    # fetch voting stats from postgres
    fetch_voting_stats()