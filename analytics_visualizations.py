#!/usr/bin/env python3
"""
Weather Data Analytics Script
Purpose: Generate comprehensive analytics, visualizations, and insights
Dependencies: pip install psycopg2-binary pandas matplotlib seaborn plotly
"""

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import json
import os

# Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5433,
    'database': 'weather_db',
    'user': 'weather_user',
    'password': 'weather_pass'
}

OUTPUT_DIR = 'analytics_output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Set visualization style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================

def get_connection():
    """Create database connection"""
    return psycopg2.connect(**DB_CONFIG)

# ============================================================================
# DATA EXTRACTION FUNCTIONS
# ============================================================================

def get_hourly_trends(conn, hours=24):
    """Get hourly temperature trends"""
    query = """
        SELECT 
            city,
            state,
            DATE_TRUNC('hour', recorded_at) as hour,
            AVG(temperature_f) as avg_temp,
            MAX(temperature_f) as max_temp,
            MIN(temperature_f) as min_temp,
            COUNT(*) as sample_count
        FROM current_weather
        WHERE recorded_at >= NOW() - INTERVAL '%s hours'
        GROUP BY city, state, DATE_TRUNC('hour', recorded_at)
        ORDER BY hour DESC, city, state
    """ % hours
    
    return pd.read_sql_query(query, conn)

def get_weekly_comparison(conn):
    """Compare current week vs last week"""
    query = """
        WITH current_week AS (
            SELECT 
                city, state,
                AVG(temperature_f) as avg_temp_current
            FROM current_weather
            WHERE recorded_at >= DATE_TRUNC('week', NOW())
            GROUP BY city, state
        ),
        last_week AS (
            SELECT 
                city, state,
                AVG(temperature_f) as avg_temp_last
            FROM current_weather
            WHERE recorded_at >= DATE_TRUNC('week', NOW() - INTERVAL '1 week')
              AND recorded_at < DATE_TRUNC('week', NOW())
            GROUP BY city, state
        )
        SELECT 
            cw.city, cw.state,
            cw.avg_temp_current,
            lw.avg_temp_last,
            (cw.avg_temp_current - lw.avg_temp_last) as temp_change,
            ((cw.avg_temp_current - lw.avg_temp_last) / NULLIF(lw.avg_temp_last, 0) * 100) as percent_change
        FROM current_week cw
        JOIN last_week lw ON cw.city = lw.city AND cw.state = lw.state
        ORDER BY ABS(cw.avg_temp_current - lw.avg_temp_last) DESC
    """
    
    return pd.read_sql_query(query, conn)

def get_weather_patterns(conn, days=30):
    """Get weather pattern distribution"""
    query = """
        SELECT 
            city, state, summary,
            COUNT(*) as occurrence_count,
            ROUND((COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY city, state))::numeric, 2) as percentage,
            AVG(temperature_f) as avg_temp_during,
            AVG(humidity) as avg_humidity
        FROM current_weather
        WHERE recorded_at >= NOW() - INTERVAL '%s days'
        GROUP BY city, state, summary
        ORDER BY city, state, occurrence_count DESC
    """ % days
    
    return pd.read_sql_query(query, conn)

def get_regional_stats(conn):
    """Get statistics by US region"""
    query = """
        WITH regional_mapping AS (
            SELECT 
                temperature_f, humidity, wind_speed_mph, summary,
                CASE 
                    WHEN state IN ('ME', 'NH', 'VT', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA') THEN 'Northeast'
                    WHEN state IN ('OH', 'IN', 'IL', 'MI', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') THEN 'Midwest'
                    WHEN state IN ('DE', 'MD', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'KY', 'TN', 'AL', 'MS', 'AR', 'LA') THEN 'South'
                    WHEN state IN ('MT', 'ID', 'WY', 'NV', 'UT', 'CO', 'AZ', 'NM', 'OK', 'TX') THEN 'Southwest'
                    WHEN state IN ('WA', 'OR', 'CA', 'AK', 'HI') THEN 'West'
                END as region
            FROM current_weather
            WHERE recorded_at >= NOW() - INTERVAL '24 hours'
        )
        SELECT 
            region,
            ROUND(AVG(temperature_f)::numeric, 2) as avg_temp,
            ROUND(MAX(temperature_f)::numeric, 2) as max_temp,
            ROUND(MIN(temperature_f)::numeric, 2) as min_temp,
            ROUND(AVG(humidity)::numeric, 2) as avg_humidity,
            ROUND(AVG(wind_speed_mph)::numeric, 2) as avg_wind_speed
        FROM regional_mapping
        WHERE region IS NOT NULL
        GROUP BY region
        ORDER BY avg_temp DESC
    """
    
    return pd.read_sql_query(query, conn)

def get_forecast_accuracy(conn, days=7):
    """Calculate forecast accuracy metrics"""
    query = """
        WITH forecast_actual AS (
            SELECT 
                fc.city, fc.state,
                ABS(fc.temperature_f - cw.temperature_f) as temp_error,
                CASE WHEN fc.forecast_summary = cw.summary THEN 1 ELSE 0 END as condition_correct
            FROM forecast_comparison fc
            LEFT JOIN current_weather cw 
                ON fc.city = cw.city 
                AND fc.state = cw.state
                AND DATE_TRUNC('hour', fc.forecast_hour) = DATE_TRUNC('hour', cw.recorded_at)
            WHERE fc.recorded_at >= NOW() - INTERVAL '%s days'
              AND cw.temperature_f IS NOT NULL
        )
        SELECT 
            city, state,
            COUNT(*) as total_forecasts,
            ROUND(AVG(temp_error)::numeric, 2) as avg_temp_error_f,
            ROUND(MAX(temp_error)::numeric, 2) as max_temp_error_f,
            ROUND((SUM(condition_correct) * 100.0 / COUNT(*))::numeric, 2) as condition_accuracy_pct
        FROM forecast_actual
        GROUP BY city, state
        HAVING COUNT(*) >= 3
        ORDER BY avg_temp_error_f ASC
    """ % days
    
    return pd.read_sql_query(query, conn)

def get_time_series_data(conn, city, state, days=7):
    """Get time series data for specific city"""
    query = """
        SELECT 
            recorded_at,
            temperature_f,
            humidity,
            wind_speed_mph,
            precipitation_inch,
            summary
        FROM current_weather
        WHERE city = %s 
          AND state = %s
          AND recorded_at >= NOW() - INTERVAL '%s days'
        ORDER BY recorded_at
    """ % (repr(city), repr(state), days)
    
    return pd.read_sql_query(query, conn)

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def plot_temperature_trends_by_city(df, top_n=10):
    """Plot temperature trends for top N cities"""
    # Get cities with most data points
    city_counts = df.groupby(['city', 'state']).size().sort_values(ascending=False).head(top_n)
    top_cities = city_counts.index.tolist()
    
    df_filtered = df[df.apply(lambda x: (x['city'], x['state']) in top_cities, axis=1)]
    df_filtered['location'] = df_filtered['city'] + ', ' + df_filtered['state']
    
    fig = px.line(
        df_filtered,
        x='hour',
        y='avg_temp',
        color='location',
        title=f'Temperature Trends - Top {top_n} Cities (Last 24 Hours)',
        labels={'hour': 'Time', 'avg_temp': 'Average Temperature (°F)'},
        markers=True
    )
    
    fig.update_layout(
        xaxis_title='Time',
        yaxis_title='Temperature (°F)',
        hovermode='x unified',
        height=600
    )
    
    fig.write_html(f'{OUTPUT_DIR}/temperature_trends.html')
    print(f"✅ Saved: {OUTPUT_DIR}/temperature_trends.html")

def plot_weekly_comparison_top_changes(df, top_n=20):
    """Bar chart of top temperature changes week-over-week"""
    df_top = df.head(top_n).copy()
    df_top['location'] = df_top['city'] + ', ' + df_top['state']
    df_top['color'] = df_top['temp_change'].apply(lambda x: 'Warming' if x > 0 else 'Cooling')
    
    fig = px.bar(
        df_top,
        x='temp_change',
        y='location',
        color='color',
        title=f'Top {top_n} Temperature Changes (Current Week vs Last Week)',
        labels={'temp_change': 'Temperature Change (°F)', 'location': 'City'},
        color_discrete_map={'Warming': 'red', 'Cooling': 'blue'},
        orientation='h'
    )
    
    fig.update_layout(
        xaxis_title='Temperature Change (°F)',
        yaxis_title='',
        height=800,
        showlegend=True
    )
    
    fig.write_html(f'{OUTPUT_DIR}/weekly_comparison.html')
    print(f"✅ Saved: {OUTPUT_DIR}/weekly_comparison.html")

def plot_regional_heatmap(df):
    """Heatmap of regional weather statistics"""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    df_pivot = df.set_index('region')[['avg_temp', 'avg_humidity', 'avg_wind_speed']]
    df_pivot.columns = ['Avg Temp (°F)', 'Avg Humidity (%)', 'Avg Wind Speed (mph)']
    
    sns.heatmap(df_pivot, annot=True, fmt='.1f', cmap='RdYlBu_r', ax=ax, cbar_kws={'label': 'Value'})
    ax.set_title('Regional Weather Statistics (Last 24 Hours)', fontsize=16, fontweight='bold')
    ax.set_xlabel('')
    ax.set_ylabel('Region', fontsize=12)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/regional_heatmap.png', dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {OUTPUT_DIR}/regional_heatmap.png")
    plt.close()

def plot_forecast_accuracy_scatter(df):
    """Scatter plot of forecast accuracy by city"""
    df['location'] = df['city'] + ', ' + df['state']
    
    fig = px.scatter(
        df,
        x='avg_temp_error_f',
        y='condition_accuracy_pct',
        size='total_forecasts',
        hover_data=['location'],
        title='Forecast Accuracy by City',
        labels={
            'avg_temp_error_f': 'Average Temperature Error (°F)',
            'condition_accuracy_pct': 'Condition Accuracy (%)',
            'total_forecasts': 'Forecast Count'
        },
        color='avg_temp_error_f',
        color_continuous_scale='RdYlGn_r'
    )
    
    fig.update_layout(height=600)
    fig.write_html(f'{OUTPUT_DIR}/forecast_accuracy.html')
    print(f"✅ Saved: {OUTPUT_DIR}/forecast_accuracy.html")

def plot_weather_pattern_sunburst(df, state='CA'):
    """Sunburst chart of weather patterns for a specific state"""
    df_state = df[df['state'] == state].copy()
    
    if len(df_state) == 0:
        print(f"⚠️  No data for state: {state}")
        return
    
    df_state['location'] = df_state['city'] + ', ' + df_state['state']
    
    fig = px.sunburst(
        df_state,
        path=['state', 'city', 'summary'],
        values='occurrence_count',
        title=f'Weather Pattern Distribution - {state}',
        color='avg_temp_during',
        color_continuous_scale='RdYlBu_r',
        labels={'avg_temp_during': 'Avg Temp (°F)'}
    )
    
    fig.update_layout(height=700)
    fig.write_html(f'{OUTPUT_DIR}/weather_patterns_{state}.html')
    print(f"✅ Saved: {OUTPUT_DIR}/weather_patterns_{state}.html")

def plot_city_time_series(df, city, state):
    """Multi-panel time series for specific city"""
    if len(df) == 0:
        print(f"⚠️  No data for {city}, {state}")
        return
    
    fig, axes = plt.subplots(3, 1, figsize=(14, 10), sharex=True)
    
    # Temperature
    axes[0].plot(df['recorded_at'], df['temperature_f'], marker='o', linewidth=2, color='crimson')
    axes[0].set_ylabel('Temperature (°F)', fontsize=12, fontweight='bold')
    axes[0].set_title(f'Weather Time Series - {city}, {state}', fontsize=16, fontweight='bold')
    axes[0].grid(True, alpha=0.3)
    
    # Humidity
    axes[1].plot(df['recorded_at'], df['humidity'], marker='s', linewidth=2, color='steelblue')
    axes[1].set_ylabel('Humidity (%)', fontsize=12, fontweight='bold')
    axes[1].grid(True, alpha=0.3)
    
    # Wind Speed
    axes[2].plot(df['recorded_at'], df['wind_speed_mph'], marker='^', linewidth=2, color='forestgreen')
    axes[2].set_ylabel('Wind Speed (mph)', fontsize=12, fontweight='bold')
    axes[2].set_xlabel('Time', fontsize=12, fontweight='bold')
    axes[2].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(f'{OUTPUT_DIR}/timeseries_{city}_{state}.png', dpi=300, bbox_inches='tight')
    print(f"✅ Saved: {OUTPUT_DIR}/timeseries_{city}_{state}.png")
    plt.close()

def generate_summary_report(conn):
    """Generate text summary report"""
    report = []
    report.append("="*80)
    report.append("WEATHER DATA ANALYTICS SUMMARY REPORT")
    report.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    report.append("="*80)
    
    # System stats
    query = "SELECT COUNT(DISTINCT city || ', ' || state) as cities FROM current_weather"
    cities = pd.read_sql_query(query, conn).iloc[0]['cities']
    
    query = "SELECT COUNT(*) as total FROM current_weather"
    total = pd.read_sql_query(query, conn).iloc[0]['total']
    
    query = "SELECT MAX(recorded_at) as latest FROM current_weather"
    latest = pd.read_sql_query(query, conn).iloc[0]['latest']
    
    report.append(f"\n📊 SYSTEM STATISTICS")
    report.append(f"   Total Cities Tracked: {cities}")
    report.append(f"   Total Weather Readings: {total:,}")
    report.append(f"   Latest Reading: {latest}")
    
    # Regional stats
    regional_df = get_regional_stats(conn)
    report.append(f"\n🗺️  REGIONAL OVERVIEW (Last 24 Hours)")
    for _, row in regional_df.iterrows():
        report.append(f"   {row['region']:12s}: {row['avg_temp']:6.2f}°F avg  "
                     f"(range: {row['min_temp']:.2f}°F - {row['max_temp']:.2f}°F)")
    
    # Forecast accuracy
    forecast_df = get_forecast_accuracy(conn, days=7)
    if len(forecast_df) > 0:
        avg_error = forecast_df['avg_temp_error_f'].mean()
        avg_accuracy = forecast_df['condition_accuracy_pct'].mean()
        report.append(f"\n🎯 FORECAST ACCURACY (Last 7 Days)")
        report.append(f"   Average Temperature Error: {avg_error:.2f}°F")
        report.append(f"   Average Condition Accuracy: {avg_accuracy:.2f}%")
        report.append(f"   Top 5 Most Accurate Cities:")
        for idx, row in forecast_df.head(5).iterrows():
            report.append(f"      {row['city']:20s}, {row['state']:2s}  "
                         f"Temp Error: {row['avg_temp_error_f']:.2f}°F  "
                         f"Accuracy: {row['condition_accuracy_pct']:.1f}%")
    
    report.append("\n" + "="*80)
    
    report_text = "\n".join(report)
    
    with open(f'{OUTPUT_DIR}/summary_report.txt', 'w') as f:
        f.write(report_text)
    
    print(report_text)
    print(f"\n✅ Saved: {OUTPUT_DIR}/summary_report.txt")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    print("\n" + "="*80)
    print("🔬 WEATHER DATA ANALYTICS & VISUALIZATION")
    print("="*80 + "\n")
    
    conn = get_connection()
    print("✅ Connected to database\n")
    
    try:
        # 1. Temperature Trends
        print("📊 Generating temperature trends visualization...")
        hourly_df = get_hourly_trends(conn, hours=24)
        if len(hourly_df) > 0:
            plot_temperature_trends_by_city(hourly_df, top_n=10)
            hourly_df.to_csv(f'{OUTPUT_DIR}/hourly_trends.csv', index=False)
            print(f"✅ Saved: {OUTPUT_DIR}/hourly_trends.csv")
        
        # 2. Weekly Comparison
        print("\n📈 Analyzing weekly temperature changes...")
        weekly_df = get_weekly_comparison(conn)
        if len(weekly_df) > 0:
            plot_weekly_comparison_top_changes(weekly_df, top_n=20)
            weekly_df.to_csv(f'{OUTPUT_DIR}/weekly_comparison.csv', index=False)
            print(f"✅ Saved: {OUTPUT_DIR}/weekly_comparison.csv")
        
        # 3. Regional Statistics
        print("\n🗺️  Computing regional statistics...")
        regional_df = get_regional_stats(conn)
        if len(regional_df) > 0:
            plot_regional_heatmap(regional_df)
            regional_df.to_csv(f'{OUTPUT_DIR}/regional_stats.csv', index=False)
            print(f"✅ Saved: {OUTPUT_DIR}/regional_stats.csv")
        
        # 4. Forecast Accuracy
        print("\n🎯 Analyzing forecast accuracy...")
        forecast_df = get_forecast_accuracy(conn, days=7)
        if len(forecast_df) > 0:
            plot_forecast_accuracy_scatter(forecast_df)
            forecast_df.to_csv(f'{OUTPUT_DIR}/forecast_accuracy.csv', index=False)
            print(f"✅ Saved: {OUTPUT_DIR}/forecast_accuracy.csv")
        
        # 5. Weather Patterns
        print("\n🌤️  Analyzing weather patterns...")
        patterns_df = get_weather_patterns(conn, days=30)
        if len(patterns_df) > 0:
            plot_weather_pattern_sunburst(patterns_df, state='CA')
            patterns_df.to_csv(f'{OUTPUT_DIR}/weather_patterns.csv', index=False)
            print(f"✅ Saved: {OUTPUT_DIR}/weather_patterns.csv")
        
        # 6. Example time series for specific city
        print("\n📅 Generating time series example (Los Angeles, CA)...")
        ts_df = get_time_series_data(conn, 'Los Angeles', 'CA', days=7)
        if len(ts_df) > 0:
            plot_city_time_series(ts_df, 'Los Angeles', 'CA')
        
        # 7. Summary Report
        print("\n📋 Generating summary report...")
        generate_summary_report(conn)
        
        print("\n" + "="*80)
        print("✅ ANALYTICS COMPLETE!")
        print(f"   All outputs saved to: {OUTPUT_DIR}/")
        print("="*80 + "\n")
        
    finally:
        conn.close()

if __name__ == "__main__":
    main()
