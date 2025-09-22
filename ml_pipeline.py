from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import accuracy_score
import joblib
import json
import numpy as np
import os

class MLPipeline:
    def __init__(self):
        self.model = None
        self.device_encoder = None
        self.os_encoder = None
        self.gender_encoder = None
        self.feature_columns = None
        
    def train_model(self):
        print("Loading data...")
        df = pd.read_csv('data/user_behavior_dataset.csv')
        print(f"Data shape: {df.shape}")
        
        # Encode categorical variables
        print("Encoding categorical variables...")
        self.device_encoder = LabelEncoder()
        self.os_encoder = LabelEncoder()
        self.gender_encoder = LabelEncoder()
        
        df['Device Model'] = self.device_encoder.fit_transform(df['Device Model'])
        df['Operating System'] = self.os_encoder.fit_transform(df['Operating System'])
        df['Gender'] = self.gender_encoder.fit_transform(df['Gender'])
        
        # Features and target
        X = df.drop(['User ID', 'User Behavior Class'], axis=1)
        y = df['User Behavior Class']
        self.feature_columns = X.columns.tolist()
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        # Train model
        self.model = RandomForestClassifier(n_estimators=100, random_state=42)
        self.model.fit(X_train, y_train)
        
        # Test accuracy
        y_pred = self.model.predict(X_test)
        accuracy = accuracy_score(y_test, y_pred)
        print(f"Model accuracy: {accuracy:.3f}")
        
        # Save model and encoders
        self.save_model()
        print("Model and encoders saved")
        
    def save_model(self):
        joblib.dump(self.model, 'user_behavior_model.pkl')
        joblib.dump(self.device_encoder, 'device_encoder.pkl')
        joblib.dump(self.os_encoder, 'os_encoder.pkl')
        joblib.dump(self.gender_encoder, 'gender_encoder.pkl')
        joblib.dump(self.feature_columns, 'feature_columns.pkl')
        
    def load_model(self):
        if not all(os.path.exists(f) for f in ['user_behavior_model.pkl', 'device_encoder.pkl', 'os_encoder.pkl', 'gender_encoder.pkl', 'feature_columns.pkl']):
            print("Model files not found. Training new model...")
            self.train_model()
            return
            
        self.model = joblib.load('user_behavior_model.pkl')
        self.device_encoder = joblib.load('device_encoder.pkl')
        self.os_encoder = joblib.load('os_encoder.pkl')
        self.gender_encoder = joblib.load('gender_encoder.pkl')
        self.feature_columns = joblib.load('feature_columns.pkl')
        print("Model loaded successfully")
        
    def predict_behavior(self, data):
        # Encode categorical features
        data_encoded = data.copy()
        data_encoded['Device Model'] = self.device_encoder.transform([data['Device Model']])[0]
        data_encoded['Operating System'] = self.os_encoder.transform([data['Operating System']])[0]
        data_encoded['Gender'] = self.gender_encoder.transform([data['Gender']])[0]
        
        # Create feature array
        features = [
            data_encoded['Device Model'],
            data_encoded['Operating System'],
            data_encoded['App Usage Time (min/day)'],
            data_encoded['Screen On Time (hours/day)'],
            data_encoded['Battery Drain (mAh/day)'],
            data_encoded['Number of Apps Installed'],
            data_encoded['Data Usage (MB/day)'],
            data_encoded['Age'],
            data_encoded['Gender']
        ]
        
        prediction = self.model.predict([features])[0]
        return int(prediction)
    
    def test_prediction(self):
        # Test data
        test_data = {
            'Device Model': 'Google Pixel 5',
            'Operating System': 'Android',
            'App Usage Time (min/day)': 393,
            'Screen On Time (hours/day)': 6.4,
            'Battery Drain (mAh/day)': 1872,
            'Number of Apps Installed': 67,
            'Data Usage (MB/day)': 1122,
            'Age': 40,
            'Gender': 'Male'
        }
        
        prediction = self.predict_behavior(test_data)
        print(f"Test prediction: {prediction}")
        return prediction
    
    def send_sample_data(self):
        producer = KafkaProducer(
            bootstrap_servers=['nowledgeable.com:9092'],
            value_serializer=lambda x: x.encode('utf-8'),
            acks='all',
            retries=3
        )
        
        # Load sample data
        df = pd.read_csv('data/user_behavior_dataset.csv')
        sample_user = df.iloc[0].to_dict()
        
        message_json = json.dumps(sample_user)
        
        try:
            future = producer.send('user_data', value=message_json)
            record_metadata = future.get(timeout=10)
            print(f"Sent user data: {sample_user['User ID']}")
        except Exception as e:
            print(f"Error: {e}")
        finally:
            producer.close()
    
    def run_prediction_consumer(self):
        consumer = KafkaConsumer(
            'user_data',
            bootstrap_servers=['nowledgeable.com:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='ml-prediction-group',
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        producer = KafkaProducer(
            bootstrap_servers=['nowledgeable.com:9092'],
            value_serializer=lambda x: x.encode('utf-8'),
            acks='all',
            retries=3
        )
        
        print("ML consumer started...")
        
        try:
            for message in consumer:
                user_data = json.loads(message.value)
                
                prediction = self.predict_behavior(user_data)
                
                result = {
                    "user_id": user_data.get("User ID", "unknown"),
                    "prediction": prediction
                }
                
                producer.send('prediction_guelennoc', value=json.dumps(result))
                print(f"Prediction: {prediction} for user {user_data.get('User ID', 'unknown')}")
                
        except KeyboardInterrupt:
            print("Stopping...")
        finally:
            consumer.close()
            producer.close()
    
    def run_prediction_viewer(self):
        consumer = KafkaConsumer(
            'prediction_guelennoc',
            bootstrap_servers=['nowledgeable.com:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='prediction-viewer-group',
            value_deserializer=lambda x: x.decode('utf-8') if x else None
        )
        
        print("Prediction viewer started...")
        
        try:
            for message in consumer:
                prediction_data = json.loads(message.value)
                print(f"User {prediction_data['user_id']}: Prediction = {prediction_data['prediction']}")
        except KeyboardInterrupt:
            print("Stopping...")
        finally:
            consumer.close()

def main():
    ml_pipeline = MLPipeline()
    
    import sys
    if len(sys.argv) < 2:
        print("Usage: python ml_pipeline.py [train|test|send|consumer|viewer]")
        return
    
    command = sys.argv[1]
    
    if command == "train":
        ml_pipeline.train_model()
    elif command == "test":
        ml_pipeline.load_model()
        ml_pipeline.test_prediction()
    elif command == "send":
        ml_pipeline.send_sample_data()
    elif command == "consumer":
        ml_pipeline.load_model()
        ml_pipeline.run_prediction_consumer()
    elif command == "viewer":
        ml_pipeline.run_prediction_viewer()
    else:
        print("Invalid command. Use: train, test, send, consumer, or viewer")

if __name__ == "__main__":
    main()
