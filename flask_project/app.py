import logging
import os
from flask import Flask, render_template, request, jsonify
import joblib
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
import mysql.connector

# Initialize Flask app
app = Flask(__name__)

# Load the trained model
rf_model = joblib.load('models/random_forest_model_v1.pkl')

logging.basicConfig(level=logging.ERROR)

def fetch_data(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch data from the database within the specified date range."""
    try:
        with mysql.connector.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', '951Kdroot@12'),
            database=os.getenv('DB_NAME', 'MLhack')
        ) as connection:
            query = """
            SELECT 
                c.customer_id,
                c.age,
                c.tenure,
                c.monthly_usage,
                SUM(CASE WHEN e.event_type = 'Complaints' THEN 1 ELSE 0 END) AS complaints,
                SUM(CASE WHEN e.event_type = 'Returns' THEN 1 ELSE 0 END) AS returns,
                SUM(CASE WHEN e.event_type = 'email_open' THEN 1 ELSE 0 END) AS emails_opened,
                SUM(CASE WHEN e.event_type = 'login' THEN 1 ELSE 0 END) AS daily_logins,
                SUM(CASE WHEN e.event_type = 'sensor_trigger' THEN 1 ELSE 0 END) AS sensor_triggers
            FROM events e
            JOIN customers c ON e.customer_id = c.customer_id
            WHERE e.event_timestamp >= %(start_date)s AND e.event_timestamp < %(end_date)s
            GROUP BY c.customer_id, c.age, c.tenure, c.monthly_usage;
            """
            events_df = pd.read_sql(query, connection, params={'start_date': start_date, 'end_date': end_date})
    except mysql.connector.Error as err:
        logging.error(f"Error: {err}")
        return pd.DataFrame()
    return events_df

def preprocess_data(events_df: pd.DataFrame) -> pd.DataFrame:
    """Preprocess the data by imputing missing values and scaling features."""
    features = ['age', 'tenure', 'monthly_usage', 'complaints', 'returns',
                'emails_opened', 'daily_logins', 'sensor_triggers']
    imputer = SimpleImputer(strategy='mean')
    scaler = StandardScaler()
    events_df[features] = imputer.fit_transform(events_df[features])
    events_df[features] = scaler.fit_transform(events_df[features])
    return events_df.rename(columns={
        'age': 'Age',
        'complaints': 'Complaints',
        'daily_logins': 'Daily_Logins',
        'emails_opened': 'Emails_Opened',
        'monthly_usage': 'Monthly_Usage',
        'sensor_triggers': 'Sensor_Triggers',
        'tenure': 'Tenure',
        'returns': 'Returns'
    })

def make_predictions(events_df: pd.DataFrame) -> list:
    """Make predictions using the pre-trained model and return at-risk customers."""
    features = ['Age', 'Tenure', 'Monthly_Usage', 'Complaints',
                'Returns', 'Emails_Opened', 'Daily_Logins', 'Sensor_Triggers']
    events_df['Churn_Prediction'] = rf_model.predict(events_df[features])
    at_risk_customers = events_df[events_df['Churn_Prediction'] == 1]
    return at_risk_customers[['customer_id']].to_dict(orient='records')

def fetch_and_predict(start_date: str, end_date: str) -> list:
    """Fetch data, preprocess it, and make predictions."""
    events_df = fetch_data(start_date, end_date)
    if events_df.empty:
        return []
    processed_df = preprocess_data(events_df)
    return make_predictions(processed_df)

@app.route('/')
def index():
    """Render the homepage."""
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    """Handle prediction requests."""
    month_dates = {
        'January': ('2024-01-01', '2024-02-01'),
        'February': ('2024-02-01', '2024-03-01'),
        'March': ('2024-03-01', '2024-04-01'),
        'April': ('2024-04-01', '2024-05-01'),
        'May': ('2024-05-01', '2024-06-01'),
        'June': ('2024-06-01', '2024-07-01'),
        'July': ('2024-07-01', '2024-08-01'),
        'August': ('2024-08-01', '2024-09-01'),
        'September': ('2024-09-01', '2024-10-01'),
        'October': ('2024-10-01', '2024-11-01'),
        'November': ('2024-11-01', '2024-12-01'),
        'December': ('2024-12-01', '2025-01-01')
    }
    month = request.json.get('month')
    start_date, end_date = month_dates.get(month, (None, None))
    if not start_date or not end_date:
        return jsonify({"error": "Invalid month"}), 400

    at_risk_customers = fetch_and_predict(start_date, end_date)
    return jsonify(at_risk_customers)

if __name__ == '__main__':
    app.run(debug=True)
