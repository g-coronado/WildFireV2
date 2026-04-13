from django.db import connection

import json
from functools import lru_cache

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

from datetime import datetime

# DATA_URL = 'https://raw.githubusercontent.com/g-coronado/mln_data/refs/heads/main/fp-historical-wildfire-data-2006-2025.csv' # delete it when the data is imported into the database


def assign_zone(lat, lon):
    if pd.isna(lat) or pd.isna(lon):
        return 'Unknown'
    if lat < 53:
        return 'South'
    elif lat < 57:
        lat_band = 'Central'
    else:
        lat_band = 'North'
    lon_band = 'West' if lon < -115 else 'East'
    return f'{lat_band} {lon_band}'


def choose_cause(row):
    for col in ['TRUE_CAUSE', 'INDUSTRY_IDENTIFIER', 'ACTIVITY_CLASS', 'RESPONSIBLE_GROUP', 'GENERAL_CAUSE']:
        value = row.get(col)
        if pd.notna(value):
            return value
    return 'Unknown'


@lru_cache(maxsize=1)
def build_dashboard_data():
    # raw_df = pd.read_csv(DATA_URL) # delete it when the data is imported into the database
    raw_df = pd.read_sql("SELECT * FROM wildfire_data", connection)

    df = raw_df.copy()

    df = df[pd.to_datetime(df['FIRE_START_DATE'], errors='coerce').notna()].copy()
    df['FIRE_START_DATE'] = pd.to_datetime(df['FIRE_START_DATE'])
    df['YEAR'] = df['FIRE_START_DATE'].dt.year
    df['MONTH'] = df['FIRE_START_DATE'].dt.month
    df['YEAR_MONTH'] = df['FIRE_START_DATE'].dt.to_period('M').astype(str)
    df = df[(df['YEAR'] >= 2006) & (df['YEAR'] <= 2025)].copy()

    df['CAUSE_FINAL'] = df.apply(choose_cause, axis=1)
    df['CAUSE_FINAL'] = df['CAUSE_FINAL'].replace(['nan', 'NaN', None, '', ' '], np.nan)
    df['CAUSE_FINAL'] = df['CAUSE_FINAL'].fillna('Lightning')

    df['WEATHER_CONDITIONS_OVER_FIRE'] = df['WEATHER_CONDITIONS_OVER_FIRE'].replace(
    ['nan', 'NaN', 'None', '', ' ', '  '], np.nan)

    df['WEATHER_CONDITIONS_OVER_FIRE'] = df['WEATHER_CONDITIONS_OVER_FIRE'].fillna('Unknown')

    df['ZONE'] = df.apply(lambda row: assign_zone(row['LATITUDE'], row['LONGITUDE']), axis=1)

    visual_cols = [
        'FIRE_START_DATE', 'YEAR', 'MONTH', 'YEAR_MONTH',
        'LATITUDE', 'LONGITUDE', 'ZONE',
        'CURRENT_SIZE', 'SIZE_CLASS',
        'GENERAL_CAUSE', 'TRUE_CAUSE', 'CAUSE_FINAL',
        'FIRE_ORIGIN', 'WEATHER_CONDITIONS_OVER_FIRE',
        'TEMPERATURE', 'RELATIVE_HUMIDITY', 'WIND_SPEED', 'WIND_DIRECTION',
        'DISPATCHED_RESOURCE'
    ]
    wildfire_visual_df = df[[col for col in visual_cols if col in df.columns]].copy()

    # --- CLEAN LAT/LON BEFORE BUILDING HEATMAP ---
    clean_df = wildfire_visual_df.copy()

    # 1. Convert LAT/LON to numeric
    clean_df['LATITUDE'] = pd.to_numeric(clean_df['LATITUDE'], errors='coerce')
    clean_df['LONGITUDE'] = pd.to_numeric(clean_df['LONGITUDE'], errors='coerce')

    # 2. Drop invalid coordinates
    clean_df = clean_df.dropna(subset=['LATITUDE', 'LONGITUDE'])

    # 3. Drop invalid fire size
    clean_df = clean_df.dropna(subset=['CURRENT_SIZE'])

    # 4. Remove coordinates outside Alberta bounds
    clean_df = clean_df[
        (clean_df['LATITUDE'] >= 48.9) &
        (clean_df['LATITUDE'] <= 60.1) &
        (clean_df['LONGITUDE'] >= -120.1) &
        (clean_df['LONGITUDE'] <= -109.9)
    ].copy()
    # Limit heatmap to last 5 years
    recent_year = clean_df['YEAR'].max() - 5
    clean_df = clean_df[clean_df['YEAR'] >= recent_year].copy()
    

    # ------------------------------------------------



    # Heatmap points from historical wildfire locations
    heatmap_points = (
        clean_df
            .apply(lambda row: [
                float(row['LATITUDE']),
                float(row['LONGITUDE']),
                min(1.0, max(0.3, float(row['CURRENT_SIZE']) / 5))
            ], axis=1)
            .tolist()
    )

    cause_counts = wildfire_visual_df.groupby('CAUSE_FINAL').size().reset_index(name='count')
    cause_counts['CAUSE_GROUPED'] = np.where(cause_counts['count'] < 100, 'Other', cause_counts['CAUSE_FINAL'])
    grouped_causes = cause_counts.groupby('CAUSE_GROUPED', as_index=False)['count'].sum().sort_values('count', ascending=False)

    zone_counts = wildfire_visual_df.groupby('ZONE').size().reset_index(name='count').sort_values('count', ascending=False)

    year_month_counts = wildfire_visual_df.groupby(['YEAR', 'MONTH', 'YEAR_MONTH']).size().reset_index(name='count')
    top_year_month = year_month_counts.sort_values('count', ascending=False).head(10)

    month_counts = wildfire_visual_df.groupby('MONTH').size().reset_index(name='count').sort_values('MONTH')

    area_per_year = wildfire_visual_df.groupby('YEAR', as_index=False)['CURRENT_SIZE'].sum()
    weather_counts = (
        wildfire_visual_df['WEATHER_CONDITIONS_OVER_FIRE']
        .fillna('Unknown')
        .value_counts()
        .reset_index()
    )
    weather_counts.columns = ['WEATHER_CONDITIONS_OVER_FIRE', 'count']

    monthly_counts = (
        wildfire_visual_df.groupby(['ZONE', pd.Grouper(key='FIRE_START_DATE', freq='MS')])
        .size()
        .reset_index(name='FIRE_COUNT')
        .rename(columns={'FIRE_START_DATE': 'DATE'})
    )

    zones = sorted(monthly_counts['ZONE'].dropna().unique())
    all_months = pd.date_range(monthly_counts['DATE'].min(), monthly_counts['DATE'].max(), freq='MS')
    full_index = pd.MultiIndex.from_product([zones, all_months], names=['ZONE', 'DATE'])

    wildfire_monthly = (
        monthly_counts.set_index(['ZONE', 'DATE'])
        .reindex(full_index, fill_value=0)
        .reset_index()
    )
    wildfire_monthly['YEAR'] = wildfire_monthly['DATE'].dt.year
    wildfire_monthly['MONTH'] = wildfire_monthly['DATE'].dt.month
    wildfire_monthly['YEAR_MONTH'] = wildfire_monthly['DATE'].dt.to_period('M').astype(str)
    wildfire_monthly = wildfire_monthly.sort_values(['ZONE', 'DATE']).copy()

    for lag in range(1, 7):
        wildfire_monthly[f'FIRE_COUNT_LAG{lag}'] = wildfire_monthly.groupby('ZONE')['FIRE_COUNT'].shift(lag)

    wildfire_monthly = wildfire_monthly.dropna().copy()

    feature_cols = [
        'MONTH',
        'FIRE_COUNT_LAG1', 'FIRE_COUNT_LAG2', 'FIRE_COUNT_LAG3',
        'FIRE_COUNT_LAG4', 'FIRE_COUNT_LAG5', 'FIRE_COUNT_LAG6',
        'ZONE'
    ]
    target = 'FIRE_COUNT'

    model_df = pd.get_dummies(wildfire_monthly[feature_cols + [target, 'DATE']], columns=['ZONE'], drop_first=True)
    cutoff_date = model_df['DATE'].max() - pd.DateOffset(months=11)
    train_df = model_df[model_df['DATE'] < cutoff_date].copy()
    test_df = model_df[model_df['DATE'] >= cutoff_date].copy()

    X_train = train_df.drop(columns=[target, 'DATE'])
    y_train = train_df[target]
    X_test = test_df.drop(columns=[target, 'DATE'])
    y_test = test_df[target]

    wildfire_model = RandomForestRegressor(
        n_estimators=400,
        max_depth=10,
        min_samples_leaf=2,
        random_state=42,
    )
    wildfire_model.fit(X_train, y_train)
    y_pred = wildfire_model.predict(X_test)

    rmse = float(np.sqrt(mean_squared_error(y_test, y_pred)))
    mae = float(mean_absolute_error(y_test, y_pred))
    r2 = float(r2_score(y_test, y_pred))

    today = datetime.now()
    forecast_date = today + pd.DateOffset(months=1)
    model_columns = X_train.columns.tolist()
    zone_dummy_cols = [col for col in model_columns if col.startswith('ZONE_')]
    next_month_predictions = []

    for zone in zones:
        zone_history = wildfire_monthly[wildfire_monthly['ZONE'] == zone].sort_values('DATE')
        last_row = zone_history.iloc[-1]
        new_data = {
            'MONTH': forecast_date.month,
            'FIRE_COUNT_LAG1': last_row['FIRE_COUNT'],
            'FIRE_COUNT_LAG2': last_row['FIRE_COUNT_LAG1'],
            'FIRE_COUNT_LAG3': last_row['FIRE_COUNT_LAG2'],
            'FIRE_COUNT_LAG4': last_row['FIRE_COUNT_LAG3'],
            'FIRE_COUNT_LAG5': last_row['FIRE_COUNT_LAG4'],
            'FIRE_COUNT_LAG6': last_row['FIRE_COUNT_LAG5'],
        }
        for col in zone_dummy_cols:
            zone_name = col.replace('ZONE_', '')
            new_data[col] = 1 if zone == zone_name else 0
        new_df = pd.DataFrame([new_data])
        for col in model_columns:
            if col not in new_df.columns:
                new_df[col] = 0
        new_df = new_df[model_columns]
        prediction = wildfire_model.predict(new_df)[0]
        next_month_predictions.append({
            'ZONE': zone,
            'FORECAST_MONTH': forecast_date.strftime('%Y-%m'),
            'PREDICTED_FIRE_COUNT': round(float(prediction), 0),
        })

    forecast_df = pd.DataFrame(next_month_predictions).sort_values('PREDICTED_FIRE_COUNT', ascending=False)

    zone_centers = (
        wildfire_visual_df
        .dropna(subset=['LATITUDE', 'LONGITUDE'])
        .groupby('ZONE', as_index=False)[['LATITUDE', 'LONGITUDE']]
        .mean()
    )
    forecast_map_df = forecast_df.merge(zone_centers, on='ZONE', how='left')

    if not forecast_map_df.empty:
        likely_threshold = float(forecast_map_df['PREDICTED_FIRE_COUNT'].quantile(0.50))
        very_likely_threshold = float(forecast_map_df['PREDICTED_FIRE_COUNT'].quantile(0.75))
    else:
        likely_threshold = 0.0
        very_likely_threshold = 0.0

    map_points = []
    for _, row in forecast_map_df.iterrows():
        prediction = float(row['PREDICTED_FIRE_COUNT'])
        if pd.isna(row.get('LATITUDE')) or pd.isna(row.get('LONGITUDE')):
            continue
        if prediction >= very_likely_threshold:
            risk_level = 'Very likely'
            color = '#dc2626'
        elif prediction >= likely_threshold:
            risk_level = 'Likely'
            color = '#eab308'
        else:
            risk_level = 'Low'
            color = '#fcd34d'   # light yellow

        radius = max(10, min(28, 10 + prediction * 1.5))
        map_points.append({
            'zone': row['ZONE'],
            'forecast_month': row['FORECAST_MONTH'],
            'predicted_fire_count': round(prediction, 0),
            'risk_level': risk_level,
            'color': color,
            'latitude': round(float(row['LATITUDE']), 4),
            'longitude': round(float(row['LONGITUDE']), 4),
            'radius': round(float(radius), 1),
        })

    return {
        'summary': {
            'total_fires': int(len(wildfire_visual_df)),
            'date_range': f"{wildfire_visual_df['YEAR'].min()} - {wildfire_visual_df['YEAR'].max()}",
            'average_fire_size': round(float(wildfire_visual_df['CURRENT_SIZE'].mean()), 2),
            'forecast_month': forecast_date.strftime('%Y-%m'),
            'top_zone': zone_counts.iloc[0]['ZONE'] if not zone_counts.empty else 'N/A',
            'top_cause': grouped_causes.iloc[0]['CAUSE_GROUPED'] if not grouped_causes.empty else 'N/A',
        },
        'metrics': {
            'rmse': round(rmse, 2),
            'mae': round(mae, 2),
            'r2': round(r2, 3),
        },
        'charts': {
            'cause': {
                'labels': grouped_causes['CAUSE_GROUPED'].astype(str).tolist(),
                'values': grouped_causes['count'].astype(int).tolist(),
            },
            'zone': {
                'labels': zone_counts['ZONE'].astype(str).tolist(),
                'values': zone_counts['count'].astype(int).tolist(),
            },
            'top_months': {
                'labels': top_year_month['YEAR_MONTH'].astype(str).tolist(),
                'values': top_year_month['count'].astype(int).tolist(),
            },
            'month': {
                'labels': month_counts['MONTH'].astype(str).tolist(),
                'values': month_counts['count'].astype(int).tolist(),
            },
            'area': {
                'labels': area_per_year['YEAR'].astype(str).tolist(),
                'values': [round(float(x), 2) for x in area_per_year['CURRENT_SIZE'].fillna(0)],
            },
            'weather': {
                'labels': weather_counts['WEATHER_CONDITIONS_OVER_FIRE'].astype(str).tolist(),
                'values': weather_counts['count'].astype(int).tolist(),
            },
            'forecast': {
                'labels': forecast_df['ZONE'].astype(str).tolist(),
                'values': forecast_df['PREDICTED_FIRE_COUNT'].astype(int).tolist(),
            },
        },
        'forecast_rows': forecast_df.to_dict(orient='records'),
        'forecast_json': json.dumps(forecast_df.to_dict(orient='records')),
        'map_points_json': json.dumps(map_points),
        'heatmap_points_json': json.dumps(heatmap_points),
    }
