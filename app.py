import streamlit as st
‎st.set_page_config(page_title="Space2Earth", layout="wide")
‎st.title("Space2Earth - Field Shift")
‎st.subheader("Peshawar Agriculture Monitoring LIVE!")
‎st.success("App is now LIVE! No errors!")
‎st.map(data=[{"lat": 34.0151, "lon": 71.5249}], zoom=10)
‎st.write("Peshawar: 34.0151, 71.5249")
‎st.balloons()
