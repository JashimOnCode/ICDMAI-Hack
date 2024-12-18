from flask import Flask, render_template, request, jsonify
import joblib
import pandas as pd
import numpy as np
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
import mysql.connector

# Initialize Flask app
app = Flask(__name__)

# Load the trained model
rf_model = joblib.load('models/random_forest_model_v1.pkl')

# Function to fetch data from the database and make predictions
def fetch_and_predict(start_date, end_date):
    # Connect to the MySQL database
    connection = mysql.connector.connect(
        host='localhost',
        user='root',
        password='951Kdroot@12',
        database='MLhack'
    )

    # SQL query to fetch events data for a specific month
    events_query = f"""
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
    WHERE e.event_timestamp >= '{start_date}' AND e.event_timestamp < '{end_date}'
    GROUP BY c.customer_id, c.age, c.tenure, c.monthly_usage;
    """

    # Fetch data using pandas
    events_df = pd.read_sql(events_query, connection)

    # Close the database connection
    connection.close()

    # Data preprocessing
    imputer = SimpleImputer(strategy='mean')
    scaler = StandardScaler()

    events_df[['age', 'tenure', 'monthly_usage', 'complaints', 'returns',
               'emails_opened', 'daily_logins', 'sensor_triggers']] = imputer.fit_transform(
        events_df[['age', 'tenure', 'monthly_usage', 'complaints', 'returns',
                   'emails_opened', 'daily_logins', 'sensor_triggers']]
    )

    events_df[['age', 'tenure', 'monthly_usage', 'complaints', 'returns',
               'emails_opened', 'daily_logins', 'sensor_triggers']] = scaler.fit_transform(
        events_df[['age', 'tenure', 'monthly_usage', 'complaints', 'returns',
                   'emails_opened', 'daily_logins', 'sensor_triggers']]
    )

    # Rename columns to match the model's expectations
    events_df = events_df.rename(columns={
        'age': 'Age',
        'complaints': 'Complaints',
        'daily_logins': 'Daily_Logins',
        'emails_opened': 'Emails_Opened',
        'monthly_usage': 'Monthly_Usage',
        'sensor_triggers': 'Sensor_Triggers',
        'tenure': 'Tenure',
        'returns': 'Returns'
    })

    # Make predictions
    X_predict = events_df[['Age', 'Tenure', 'Monthly_Usage', 'Complaints',
                           'Returns', 'Emails_Opened', 'Daily_Logins', 'Sensor_Triggers']]
    events_df['Churn_Prediction'] = rf_model.predict(X_predict)

    # Return only at-risk customers
    at_risk_customers = events_df[events_df['Churn_Prediction'] == 1]
    return at_risk_customers[['customer_id']].to_dict(orient='records')

# Route for the homepage
@app.route('/')
def index():
    return render_template('index.html')

# Route for predictions
@app.route('/predict', methods=['POST'])
def predict():
    month = request.json.get('month')
    if month == 'January':
        start_date = '2024-01-01'
        end_date = '2024-02-01'
    # Add similar conditions for other months

    # Fetch and predict
    at_risk_customers = fetch_and_predict(start_date, end_date)
    return jsonify(at_risk_customers)

if __name__ == '__main__':
    app.run(debug=True)
