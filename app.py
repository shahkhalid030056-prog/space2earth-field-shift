import streamlit as st‎
‎st.set_page_config(page_title="Peshawar Agri Monitor", layout="wide")
‎st.title("Peshawar Agriculture Monitoring LIVE!")
‎st.write("App is now LIVE! No errors!")
‎st.metric("Temperature", "32C")
‎st.map(data=[{"lat": 34.0151, "lon": 71.5249}])
‎st.success("Project working!")
‎st.balloons()
