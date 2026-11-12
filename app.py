‎
‎import streamlit as st
‎import requests, folium
‎from streamlit_folium import st_folium
‎
‎st.set_page_config(page_title="Space2Earth Peshawar", page_icon="🌾", layout="wide")
‎st.title("🌾 Space2Earth - Field Shift (Peshawar)")
‎st.markdown("**Bilingual Crop Rotation + NASA POWER | Independent Project**")
‎
‎st.success("✅ FIXED! Real NASA Data - Streamlit Ready")
‎
‎m = folium.Map(location=[34.0151,71.5249], zoom_start=5, tiles="OpenStreetMap")
‎folium.Marker([34.0151,71.5249], tooltip="Peshawar Ground Station", icon=folium.Icon(color="green", icon="home")).add_to(m)
‎st_folium(m, width=700, height=500)
‎
‎st.header("NASA POWER - Peshawar Temp")
‎if st.button("Get NASA POWER Data"):
‎    try:
‎        url = "https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M&community=AG&longitude=71.5249&latitude=34.0151&start=20200101&end=20200105&format=JSON"
‎        data = requests.get(url, timeout=20).json()
‎        st.json(data)
‎        st.success("Real NASA POWER Fetched!")
‎    except Exception as e:
‎        st.error(str(e))
‎
‎st.info("Built by Shah Khalid, Peshawar - Solo | +923314133337")
‎
