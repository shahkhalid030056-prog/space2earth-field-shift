# Space2Earth - Field Shift (Peshawar)

Bilingual (English/Urdu) crop-rotation and climate-risk explorer for small farmers.

Challenge: Field Shift - Adapting Farms with NASA Data - NASA Space Apps 2026

## What it does
- Live Conditions & 7-Day Forecast for Peshawar / Any Lat-Lon
- Crop Rotation Assessment - Wheat -> Mustard suggestion
- Uses NASA POWER API - Live 7-day temp/rain, water balance, heat/frost risk
- Uses Open-Meteo for forecast - Clearly labelled, NOT NASA data
- Inputs: soil type, crop type, latitude, longitude
- Features: Metrics, 7 Daily Graphs, Soil Moisture, NDVI Estimate, JSON Download, Urdu/English

## Honest Limits
- NASA POWER is historical (2 days lag) + Open-Meteo is forecast model.
- Soil moisture is model output, not sensor data. Soil type is assumed.
- NDVI is an estimate only. Real MODIS / SMAP / GPM = future work via Earthdata.
- No false flood alerts. Forecasts are model predictions, not observations.

## Tech
Python, Flask + Gunicorn

## How to run
pip install flask requests
python app.py

## Built by
Shah Khalid, Peshawar - Solo, built with AI help (Meta AI + Claude)

## Note
Independent project, not affiliated with NASA.

## License
Apache-2.0
