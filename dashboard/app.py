import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
import os
from datetime import datetime
import altair as alt

# Page Configuration
st.set_page_config(
    page_title="Seoul Bike Pipeline Monitor",
    page_icon="🚲",
    layout="wide"
)

# Database Connection
@st.cache_resource
def get_db_engine():
    user = os.getenv("POSTGRES_USER", "airflow")
    password = os.getenv("POSTGRES_PASSWORD", "airflow")
    host = os.getenv("POSTGRES_HOST", "postgres-warehouse")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "bike_warehouse")
    
    url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)

# Data Fetching
def fetch_data():
    engine = get_db_engine()
    
    # 1. Freshness Check
    freshness_query = text("""
        SELECT MAX(created_at) as last_update 
        FROM public_marts.fct_bike_status
    """)
    
    # 2. Key Metrics
    metrics_query = text("""
        SELECT 
            COUNT(*) as total_rows,
            AVG(load_rate) as avg_load_rate,
            SUM(bike_count) as total_bikes
        FROM public_marts.fct_bike_status
        WHERE created_at = (SELECT MAX(created_at) FROM public_marts.fct_bike_status)
    """)
    
    # 3. Weather Metrics (Realtime)
    weather_query = text("""
        SELECT 
            base_date, base_time,
            MAX(CASE WHEN category = 'TMP' THEN fcst_value END) as current_temp,
            MAX(CASE WHEN category = 'PTY' THEN fcst_value END) as precip_type -- 0:None, 1:Rain, 2:Sleet, 3:Snow, 4:Shower
        FROM raw_data.weather_realtime
        WHERE (base_date, base_time) = (
            SELECT base_date, base_time 
            FROM raw_data.weather_realtime 
            ORDER BY base_date DESC, base_time DESC 
            LIMIT 1
        )
        GROUP BY base_date, base_time
    """)

    # 4. Geo Data (Latest Snapshot)
    geo_query = text("""
        SELECT 
            s.station_name,
            s.latitude,
            s.longitude,
            f.bike_count,
            f.load_rate
        FROM public_marts.fct_bike_status f
        JOIN public_marts.dim_station s ON f.station_id = s.station_id
        WHERE f.created_at = (SELECT MAX(created_at) FROM public_marts.fct_bike_status)
    """)
    
    # 5. Combined Chart Data (Last 24h)
    chart_query = text("""
        SELECT 
            datum_hour,
            avg_utilization_rate,
            temp,
            rain_type
        FROM public_marts.fct_bike_weather_hourly
        WHERE datum_hour >= NOW() - INTERVAL '24 hours'
        ORDER BY datum_hour ASC
    """)
    
    with engine.connect() as conn:
        last_update = conn.execute(freshness_query).scalar()
        metrics = conn.execute(metrics_query).mappings().fetchone()
        weather_metrics = conn.execute(weather_query).mappings().fetchone()
        geo_df = pd.read_sql(geo_query, conn)
        chart_df = pd.read_sql(chart_query, conn)
        
    return last_update, metrics, weather_metrics, geo_df, chart_df

# UI Layout
st.title("🚲 서울 따릉이 데이터 파이프라인 모니터")

try:
    with st.spinner('데이터 웨어하우스에서 최신 데이터를 가져오는 중...'):
        last_update, metrics, weather_metrics, geo_df, chart_df = fetch_data()

    # Section 1: Data Freshness & Weather
    st.header("⏱ 파이프라인 상태 & 날씨")
    
    m_col1, m_col2, m_col3 = st.columns(3)
    
    if last_update:
        time_diff = datetime.utcnow() - last_update
        m_col1.metric(
            label="최근 데이터 업데이트 (UTC)", 
            value=str(last_update),
            delta=f"{time_diff.seconds // 60}분 전",
            delta_color="normal" if time_diff.seconds < 1200 else "inverse"
        )
    else:
        m_col1.error("데이터가 없습니다!")

    if weather_metrics:
        # PTY: 0=없음, 1=비, 2=비/눈, 3=눈, 4=소나기
        pty_map = {0: "맑음 ☀️", 1: "비 🌧", 2: "비/눈 🌨", 3: "눈 ❄️", 4: "소나기 ☔️"}
        pty_val = int(weather_metrics['precip_type'] or 0)
        condition = pty_map.get(pty_val, "알 수 없음")
        
        m_col2.metric("서울 기온", f"{weather_metrics['current_temp']} °C")
        m_col3.metric("날씨 상태", condition)

    # Section 2: Key Stats
    st.divider()
    col1, col2, col3 = st.columns(3)
    col1.metric("활성 자전거 수", f"{int(metrics['total_bikes']):,}")
    col2.metric("평균 대여소 거치율", f"{metrics['avg_load_rate']:.1f}%")
    col3.metric("모니터링 대여소 수", f"{len(geo_df):,}")

    # Section 3: Weather & Bike Correlation Analysis (NEW)
    st.divider()
    st.subheader("🌦 날씨 영향 분석 (최근 24시간)")
    st.markdown("**자전거 이용률(%)**과 **기온/강수** 데이터 비교")
    
    if not chart_df.empty:
        # Create Altair Combined Chart
        base = alt.Chart(chart_df).encode(
            x=alt.X('datum_hour:T', title='시간 (Hour)')
        )

        # Bar Chart for Temperature (Color by Rain Type)
        bars = base.mark_bar(opacity=0.3).encode(
            y=alt.Y('temp:Q', title='기온 (°C)'),
            color=alt.condition(
                alt.datum.rain_type > 0,
                alt.value('blue'),  # Blue if raining
                alt.value('orange') # Orange if sunny
            ),
            tooltip=['datum_hour', 'temp', 'rain_type']
        )

        # Line Chart for Bike Utilization
        line = base.mark_line(color='green', strokeWidth=3).encode(
            y=alt.Y('avg_utilization_rate:Q', title='평균 이용률 (%)', scale=alt.Scale(domain=[0, 100])),
            tooltip=['datum_hour', 'avg_utilization_rate']
        )

        combined_chart = alt.layer(bars, line).resolve_scale(
            y='independent'
        ).properties(
            title="최근 24시간: 날씨 vs 자전거 이용률"
        )
        
        st.altair_chart(combined_chart, use_container_width=True)
    else:
        st.info("상관관계 분석을 위한 데이터가 충분하지 않습니다.")

    # Section 4: Map
    st.subheader("📍 실시간 대여소 현황")
    st.map(geo_df, size=20, color='#00CC00')

    # Top Stations
    st.subheader("📊 혼잡 대여소 Top 20")
    top_stations = geo_df.sort_values(by='load_rate', ascending=False).head(20)
    st.bar_chart(top_stations.set_index('station_name')['load_rate'])
    
    # Raw Data Peek
    with st.expander("원본 데이터 보기"):
        st.dataframe(geo_df.head(100))

except Exception as e:
    st.error(f"데이터 웨어하우스 연결 실패. \n에러: {e}")
    st.info("'postgres-warehouse' 컨테이너가 실행 중인지 확인하세요.")

if st.button('데이터 새로고침'):
    st.rerun()
