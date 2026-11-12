import streamlit as st
‎import folium
‎from streamlit_folium import st_folium
‎
‎st.set_page_config(page_title="Space2Earth - Field Shift", layout="wide")
‎
‎st.title("🌾 Space2Earth - Field Shift")
‎st.markdown("### Peshawar Agriculture Monitoring")
‎
‎Peshawar coordinates
‎lat, lon = 34.0151, 71.5249
‎
‎m = folium.Map(location=[lat, lon], zoom_start=12)
‎folium.Marker(
‎    [lat, lon], 
‎    popup="Peshawar Field",
‎    tooltip="Your Farm Location"
‎).add_to(m)
‎
‎st_folium(m, width=700, height=500)
‎
‎st.success("✅ App is LIVE! No more Flask error!")
‎st.write("Your satellite monitoring will be shown here.")
