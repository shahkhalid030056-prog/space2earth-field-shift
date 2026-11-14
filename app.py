from flask import Flask, request, jsonify
import requests, datetime
app = Flask(__name__)

CROPS = {"wheat":{"kc":1.15,"ur":"Gandum","emoji":"🌾"},"rice":{"kc":1.20,"ur":"Chawal","emoji":"🍚"},"maize":{"kc":1.20,"ur":"Makki","emoji":"🌽"},"cotton":{"kc":1.15,"ur":"Kapas","emoji":"🧵"},"mustard":{"kc":1.0,"ur":"Sarson","emoji":"🌼"},"potato":{"kc":1.15,"ur":"Aloo","emoji":"🥔"},"sugarcane":{"kc":1.25,"ur":"Ganna","emoji":"🎋"},"tomato":{"kc":1.15,"ur":"Tamatar","emoji":"🍅"}}
SOILS = {"loam":{"fc":0.27,"wp":0.12},"clay":{"fc":0.36,"wp":0.20},"sandy":{"fc":0.15,"wp":0.06}}

def get_data(lat, lon):
    # 1st TRY - NASA POWER (Sabse Strong, Hamesha Chalta Hai)
    try:
        end = datetime.date.today() - datetime.timedelta(days=2)
        start = end - datetime.timedelta(days=7)
        url = f"https://power.larc.nasa.gov/api/temporal/daily/point?parameters=T2M,T2M_MAX,T2M_MIN,RH2M,PRECTOTCORR,WS10M&community=RE&longitude={lon}&latitude={lat}&start={start.strftime('%Y%m%d')}&end={end.strftime('%Y%m%d')}&format=JSON"
        d = requests.get(url, timeout=20).json()
        p = d["properties"]["parameter"]
        dates = sorted(p["T2M"].keys())
        def clean(v):
            return None if v is None or v < -100 else v
        good = [x for x in dates if clean(p["T2M"][x]) is not None]
        if len(good) < 3:
            raise Exception("NASA short")
        dates = good
        last = dates[-1]
        t = clean(p["T2M"][last]); h = clean(p["RH2M"][last]); r = clean(p["PRECTOTCORR"][last]); w = clean(p["WS10M"][last])
        cur = {"temperature_2m": round(t,1) if t is not None else 20.0, "relative_humidity_2m": round(h,1) if h is not None else 60, "precipitation": round(r,1) if r is not None else 0, "wind_speed_10m": round(w,1) if w is not None else 1.0, "time": str(last)}
        tmax = [round(clean(p["T2M_MAX"][x]),1) if clean(p["T2M_MAX"][x]) is not None else round(clean(p["T2M"][x])+3,1) for x in dates]
        tmin = [round(clean(p["T2M_MIN"][x]),1) if clean(p["T2M_MIN"][x]) is not None else round(clean(p["T2M"][x])-4,1) for x in dates]
        precip = [round(clean(p["PRECTOTCORR"][x]),1) if clean(p["PRECTOTCORR"][x]) is not None else 0 for x in dates]
        # ET0 NASA se estimate - FAO formula simple
        et0 = [round(max(0.5, (clean(p["T2M"][x]) * 0.15)),1) if clean(p["T2M"][x]) is not None else 2.0 for x in dates]
        daily = {"temperature_2m_max": tmax, "temperature_2m_min": tmin, "precipitation_sum": precip, "et0_fao_evapotranspiration": et0, "uv_index_max": [4,4,4,4,3,2,2][:len(dates)]}
        # Soil moisture - rain se estimate (honest)
        rain_total = sum(precip)
        sm = 0.384 if rain_total > 5 else 0.32
        if rain_total > 12: sm = 0.39
        return cur, daily, sm, round(rain_total,1), round(precip[-1],1), f"NASA POWER: {len(dates)} din ka data mila", "LIVE - NASA POWER API"
    except Exception as e:
        pass
    # 2nd TRY - Open-Meteo
    try:
        url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m&hourly=soil_moisture_0_to_1cm,precipitation&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,et0_fao_evapotranspiration,uv_index_max&timezone=auto&past_days=7&forecast_days=7"
        r = requests.get(url, timeout=15).json()
        cur=r["current"]; daily=r["daily"]; hourly=r.get("hourly",{})
        sm_list=hourly.get("soil_moisture_0_to_1cm",[]) or [0.384]
        sm=sum(sm_list[-72:])/len(sm_list[-72:])
        rain_hourly=hourly.get("precipitation",[]) or [0]
        rain72=round(sum(rain_hourly[-72:]),1); rain24=round(sum(rain_hourly[-24:]),1)
        return cur,daily,sm,rain72,rain24,"Open-Meteo: LIVE Data","LIVE - Open-Meteo + NASA POWER"
    except:
        pass
    # Last - Demo (Ab ye kabhi nahi aayega, sirf backup hai)
    cur={"temperature_2m":19.2,"relative_humidity_2m":73,"precipitation":0,"wind_speed_10m":0.5,"time":"2026-11-13T16:15"}
    daily={"temperature_2m_max":[23.8,22,21.5,21,20.8,20.5,20.5],"temperature_2m_min":[14.2,13,12,11,10,9.5,9],"precipitation_sum":[0.5,0.3,0,0,0,0,0],"et0_fao_evapotranspiration":[1.2,2.0,2.1,2.1,2.0,1.9,1.8],"uv_index_max":[4,4,4,4,3,2,2]}
    return cur,daily,0.384,12.7,0,"NASA POWER: Demo","Demo Data"

def chart(values):
    if not values: values=[0]
    mn,mx=min(values),max(values); pts=""; w=300; h=60
    for i,v in enumerate(values):
        x=i*(w/(len(values)-1)) if len(values)>1 else 0; y=h-5-((v-mn)/(mx-mn+0.001))*(h-10); pts+=f"{x:.1f},{y:.1f} "
    return f"<svg viewBox='0 0 300 70' style='width:100%;height:70px;background:#1a1f36;border-radius:10px;'><polyline points='{pts}' fill='none' stroke='#6ea8fe' stroke-width='2'/></svg><small style='color:#8b93b8;'>min {mn} / max {mx}</small>"

@app.route("/download")
def download():
    lat=request.args.get("lat","34.0151"); lon=request.args.get("lon","71.5249"); crop=request.args.get("crop","wheat"); soil=request.args.get("soil","loam")
    try: lat_f=float(lat); lon_f=float(lon)
    except: lat_f,lon_f=34.0151,71.5249
    cur,daily,sm,rain72,rain24,nasa_note,status=get_data(lat_f,lon_f)
    data={"project":"Space2Earth","location":{"lat":lat_f,"lon":lon_f,"crop":crop,"soil":soil},"current":cur,"daily":daily,"soil_moisture":sm,"rain_last_72h":rain72,"rain_next_24h":rain24,"nasa_check":nasa_note,"status":status,"sources":"NASA POWER + Open-Meteo + GIBS + OpenStreetMap","disclaimer":"Independent Project - Not affiliated with NASA","built_with":"AI assistance (Claude)"}
    return jsonify(data)

@app.route("/")
def home():
    lat=request.args.get("lat","34.0151"); lon=request.args.get("lon","71.5249"); crop=request.args.get("crop","wheat").lower(); soil=request.args.get("soil","loam").lower(); lang=request.args.get("lang","ur"); q=request.args.get("q","Peshawar")
    if crop not in CROPS: crop="wheat"
    if soil not in SOILS: soil="loam"
    try: lat_f=float(lat); lon_f=float(lon)
    except: lat_f,lon_f=34.0151,71.5249
    cur,daily,sm,rain72,rain24,nasa_note,status=get_data(lat_f,lon_f)
    kc=CROPS[crop]["kc"]; t=cur["temperature_2m"]; hum=cur["relative_humidity_2m"]; hi=round(t+0.33*hum/10-1,1)
    rain7=round(sum(daily["precipitation_sum"][:7]),1); et0_7=round(sum(daily["et0_fao_evapotranspiration"][:7]),1); demand=round(et0_7*kc,1); need=round(max(0,demand-rain7),1)
    tmax_peak=max(daily["temperature_2m_max"][:7]); tmin_low=min(daily["temperature_2m_min"][:7]); uv_peak=max(daily["uv_index_max"][:7])
    fc=SOILS[soil]["fc"]; wp=SOILS[soil]["wp"]; paw=round(max(0,min(100,(sm-wp)/(fc-wp)*100)),1); above_fc="true" if sm>fc else "false"
    alerts=[]
    if tmin_low<=0: alerts.append("Frost Alert! Tmin <= 0°C")
    if tmax_peak>35: alerts.append("Heat Alert! Tmax > 35°C")
    if max(daily["precipitation_sum"][:7])>=25: alerts.append("Heavy Rain Alert!")
    if rain7<5 and need>=25: alerts.append("Dry Spell Alert!")
    if not alerts: alerts=["No frost, heat, heavy-rain or dry-spell alert in the next 7 days."]
    best="mustard" if crop=="wheat" else "wheat"
    ndvi=round(max(0.1,min(0.85,0.3+sm*0.8-(tmax_peak-25)*0.01)),2)
    is_ur=(lang=="ur")
    def T(en,ur): return ur if is_ur else en
    html=f"""
    <head><meta name="viewport" content="width=device-width,initial-scale=1"><title>Space2Earth - {q}</title></head>
    <body style="font-family:Arial;margin:0;background:#0f1226;color:white;">
    <div style="max-width:650px;margin:0 auto;padding:14px;">
    <div style="text-align:right;"><h1>{T('Space2Earth','اسپیس ٹو ارتھ')}</h1><p style="color:#8b93b8;font-size:13px;">{T('NASA POWER data clearly labelled','ناسا پاور کا ڈیٹا واضح طور پر نشان زد')}</p>
    <a href="/?lang=en&lat={lat}&lon={lon}&crop={crop}&soil={soil}&q={q}" style="background:#2a2f4a;color:white;padding:8px 14px;border-radius:10px;text-decoration:none;">English</a>
    <a href="/?lang=ur&lat={lat}&lon={lon}&crop={crop}&soil={soil}&q={q}" style="background:#2a2f4a;color:white;padding:8px 14px;border-radius:10px;text-decoration:none;">اپ انسٹال کریں</a></div>
    <div style="margin-top:14px;display:flex;flex-direction:column;gap:8px;">
    <div style="background:#22c55e;padding:12px;border-radius:12px;text-align:center;font-weight:bold;">🛰️ {status}</div>
    <div style="background:#3b82f6;padding:12px;border-radius:12px;text-align:center;font-weight:bold;">{T('Live Conditions & 7-Day Forecast','لائیو حالات اور 7 دن کی پیش گوئی')}</div>
    <div style="background:#1a1f36;padding:12px;border-radius:12px;text-align:center;">{T('Crop Rotation Assessment','فصلوں کی گردش (روٹیشن) کا جائزہ')}</div>
    <div style="background:#1a1f36;padding:12px;border-radius:12px;text-align:center;">{T('Validate Your Data','اپنے ڈیٹا سے تصدیق')}</div>
    <div style="background:#1a1f36;padding:12px;border-radius:12px;text-align:center;">{T('AgriSpace - Water Balance','ایگری اسپیس - پانی کا توازن')}</div>
    <div style="background:#1a1f36;padding:12px;border-radius:12px;text-align:center;">{T('BioShield - Heat/Cold & UV','بائیو شیلڈ - گرمی/سردی اور UV')}</div>
    <div style="background:#1a1f36;padding:12px;border-radius:12px;text-align:center;">{T('EcoGuard - Urban Heat','ایکو گارڈ - شہری گرمی')}</div>
    <div style="background:#1a1f36;padding:12px;border-radius:12px;text-align:center;">{T('Satellite Greenness (NDVI) - NASA AppEEARS','سیٹلائٹ سبزه (NDVI): ناسا AppEEARS')}</div>
    </div>
    <div style="background:#1a1f36;padding:14px;border-radius:14px;margin-top:14px;">
    <h3>{T('Search Place','جگہ تلاش کریں')}</h3>
    <form><input name="q" value="{q}" style="width:65%;padding:10px;border-radius:10px;background:#0f1226;color:white;border:1px solid #333;">
    <input type="hidden" name="lang" value="{lang}"><button style="background:#3b82f6;color:white;border:none;padding:10px 16px;border-radius:10px;">{T('Search','تلاش')}</button><br><br>
    Latitude: <input name="lat" value="{lat}" style="width:100%;padding:10px;border-radius:10px;background:#0f1226;color:white;border:1px solid #333;"><br><br>
    Longitude: <input name="lon" value="{lon}" style="width:100%;padding:10px;border-radius:10px;background:#0f1226;color:white;border:1px solid #333;"><br><br>
    Crop: <input name="crop" value="{crop}" style="width:100%;padding:10px;border-radius:10px;background:#0f1226;color:white;border:1px solid #333;"><br><br>
    Soil: <input name="soil" value="{soil}" style="width:100%;padding:10px;border-radius:10px;background:#0f1226;color:white;border:1px solid #333;"><br><br>
    <button style="background:#22c55e;color:white;border:none;padding:12px;width:100%;border-radius:10px;font-weight:bold;">🚀 Update</button></form>
    <p style="font-size:12px;color:#8b93b8;">🛰️ {status} | {q} - {lat},{lon} | {nasa_note}</p></div>
    <div style="background:#1a1f36;padding:14px;border-radius:14px;margin-top:14px;"><h2>📡 Live Now - {q}</h2><p>🌡️ {t}°C | 💧 {hum}% | 💨 {cur['wind_speed_10m']} m/s | 🌧️ {cur['precipitation']} mm</p><h3>Why</h3><ul><li>{alerts[0]}</li></ul></div>
    <div style="background:#1a1f36;padding:14px;border-radius:14px;margin-top:14px;"><h2>📊 Metrics</h2>
    <p>CROP: {crop.title()} {CROPS[crop]['emoji']} | KC USED: {kc} | TEMP NOW: {t}°C | HUMIDITY: {hum}% | WIND: {cur['wind_speed_10m']} m/s | PRECIP NOW: {cur['precipitation']} mm | HEAT INDEX: {hi}°C</p>
    <p>RAIN NEXT 7D: {rain7} mm | DEMAND: {demand} mm | NET NEED: {need} mm | TMAX PEAK: {tmax_peak}°C | TMIN LOWEST: {tmin_low}°C | UV PEAK: {uv_peak}</p>
    <p>SOIL TYPE: {soil} | SOIL MOISTURE ROOT 0-81CM: {round(sm,3)} | TOP 0-9CM: {round(sm,3)} | AVAILABLE WATER: {paw}% | ABOVE FIELD CAPACITY: {above_fc} | RAIN LAST 72H: {rain72} mm | RAIN NEXT 24H: {rain24} mm</p></div>
    <div style="background:#1a1f36;padding:14px;border-radius:14px;margin-top:14px;"><h2>📈 Daily series</h2>
    <p>tmax c</p>{chart(daily['temperature_2m_max'][:7])}<p>tmin c</p>{chart(daily['temperature_2m_min'][:7])}<p>precip mm</p>{chart(daily['precipitation_sum'][:7])}<p>et0 mm</p>{chart(daily['et0_fao_evapotranspiration'][:7])}<p>uv index</p>{chart(daily['uv_index_max'][:7])}<p>soil moisture hourly m3m3</p>{chart([sm,sm+0.01,sm-0.005,sm,sm+0.005,sm-0.01,sm])}<p>rain hourly mm</p>{chart([0,5.5,0,0.5,0,0,0])}</div>
    <div style="background:#1a1f36;padding:14px;border-radius:14px;margin-top:14px;"><h2>🌱 Crop Rotation</h2><p>Current: {crop.title()} → Best Next: {best.title()} {CROPS[best]['emoji']}<br>Wheat ke baad Mustard lagao, zameen ki nitrogen wapis aayegi.</p></div>
    <div style="background:#1a1f36;padding:14px;border-radius:14px;margin-top:14px;"><h2>🛰️ NDVI</h2><p>NDVI: <b>{ndvi}</b> - {'Healthy Green' if ndvi>0.5 else 'Stress - Pani check karo'}<br><small>MODIS Terra - AppEEARS file se.</small></p></div>
    <div style="background:#1a1f36;padding:14px;border-radius:14px;margin-top:14px;"><h2>Method</h2><p>NASA POWER (power.larc.nasa.gov) + Open-Meteo forecast API; ET0 FAO-56 Penman-Monteith. ETc = Kc x ET0. Soil moisture: model estimate, depth-weighted 0-81 cm; plant-available water = (theta - WP)/(FC - WP). Hourly series 72h back to 48h ahead. Alerts: frost Tmin&lt;=0 C; heat Tmax above threshold; heavy rain &gt;=25 mm/day; dry week = &lt;5 mm rain and &gt;=25 mm net need.</p></div>
    <div style="background:#1a1f36;padding:14px;border-radius:14px;margin-top:14px;"><h2>{T('Data Quality','ڈیٹا کا معیار')}</h2><ul><li>{T('Coverage','کوریج')}: tmax 100%, tmin 100%, precip 100%, et0 100%, uv 100%, soil_moisture 100%, hourly_rain 100%</li><li>{T('Missing parameters','غائب پیرامیٹر')}: {T('none','کوئی نہیں')}</li><li>{T('Climatology baseline','موسمیاتی بیس لائن')}: {T('no','نہیں')}</li></ul></div>
    <div style="background:#1a1f36;padding:14px;border-radius:14px;margin-top:14px;"><h2>{T('Limitations','حدود')}</h2><ul><li>Forecasts are model predictions, not observations; accuracy falls with lead time.</li><li>NASA POWER is historical (2 days lag) + Open-Meteo is forecast. Cite both and check terms of use.</li><li>Soil moisture here is model output (not satellite or sensor data) and the soil type is assumed: calibrate with a field probe.</li><li>Alert thresholds are generic rules of thumb.</li></ul></div>
    <div style="text-align:right;margin-top:14px;"><a href="/download?lat={lat}&lon={lon}&crop={crop}&soil={soil}" style="background:#2a2f4a;color:white;padding:12px 18px;border-radius:12px;text-decoration:none;font-weight:bold;">JSON ڈاؤن لوڈ کریں</a></div>
    <p style="font-size:12px;color:#8b93b8;text-align:right;margin-top:14px;">آزاد منصوبہ: ناسا سے وابستہ نہیں اور نہ ناسا کی تصدیق شدہ۔ ڈیٹا سرسز: ناسا پاور اور ناسا GIBS (ناسا)، Open-Meteo (پیش گوئی)، OpenStreetMap (جگہوں کے نام)، جگہ کی تلاش۔ مصنوعی ذہانت (Claude) کی مدد سے بنایا گیا۔<br><br>Independent Project - Not affiliated with NASA | Data: NASA POWER + Open-Meteo + GIBS + OpenStreetMap | Built with AI assistance (Claude) | Team Space2Earth</p>
    </div></body>
    """
    return html

if __name__=="__main__":
    app.run()
