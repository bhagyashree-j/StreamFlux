import logging
import smtplib
import json
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AlertSystem:
    """
    Alert system for notifying about detected anomalies
    """
    
    def __init__(self):
        """Initialize the alert system"""
        # Email configuration
        self.email_enabled = os.getenv('EMAIL_ALERTS_ENABLED', 'false').lower() == 'true'
        self.email_sender = os.getenv('EMAIL_SENDER')
        self.email_password = os.getenv('EMAIL_PASSWORD')
        self.email_recipients = os.getenv('EMAIL_RECIPIENTS', '').split(',')
        self.email_server = os.getenv('EMAIL_SERVER', 'smtp.gmail.com')
        self.email_port = int(os.getenv('EMAIL_PORT', '587'))
        
        # Webhook configuration
        self.webhook_enabled = os.getenv('WEBHOOK_ALERTS_ENABLED', 'false').lower() == 'true'
        self.webhook_url = os.getenv('WEBHOOK_URL')
        
        # Alert thresholds
        self.min_severity = float(os.getenv('ALERT_MIN_SEVERITY', '0.7'))
        self.alert_cooldown_minutes = int(os.getenv('ALERT_COOLDOWN_MINUTES', '15'))
        
        # Track recent alerts to prevent flooding
        self.recent_alerts = {}
        
    def send_alert(self, anomaly, severity=1.0):
        """
        Send an alert for a detected anomaly
        
        Args:
            anomaly: Dictionary containing anomaly information
            severity: Severity score from 0.0 to 1.0
        
        Returns:
            Boolean indicating if alert was sent
        """
        # Check if severity meets threshold
        if severity < self.min_severity:
            logger.debug(f"Anomaly severity {severity} below threshold, not sending alert")
            return False
        
        # Check for alert cooldown
        alert_key = f"{anomaly.get('source', 'unknown')}:{anomaly.get('id', 'unknown')}"
        current_time = datetime.now()
        
        if alert_key in self.recent_alerts:
            last_alert_time = self.recent_alerts[alert_key]
            time_diff = (current_time - last_alert_time).total_seconds() / 60
            
            if time_diff < self.alert_cooldown_minutes:
                logger.debug(f"Alert for {alert_key} in cooldown period, not sending")
                return False
        
        # Update alert timestamp
        self.recent_alerts[alert_key] = current_time
        
        # Format alert message
        alert_msg = self._format_alert_message(anomaly, severity)
        
        # Send alerts through configured channels
        success = False
        
        if self.email_enabled:
            email_success = self._send_email_alert(alert_msg, anomaly)
            success = success or email_success
        
        if self.webhook_enabled:
            webhook_success = self._send_webhook_alert(alert_msg, anomaly)
            success = success or webhook_success
        
        if not self.email_enabled and not self.webhook_enabled:
            # Just log the alert if no channels are enabled
            logger.warning(f"ALERT: {alert_msg}")
            success = True
        
        return success
    
    def _format_alert_message(self, anomaly, severity):
        """Format an alert message based on the anomaly type"""
        severity_str = "HIGH" if severity > 0.9 else "MEDIUM" if severity > 0.7 else "LOW"
        
        # Base alert information
        source = anomaly.get('source', 'unknown')
        timestamp = anomaly.get('timestamp', datetime.now().isoformat())
        anomaly_score = anomaly.get('anomaly_score', 0)
        
        # Format message based on source type
        if source == 'sensor_simulator':
            sensor_id = anomaly.get('sensor_id', 'unknown')
            sensor_type = anomaly.get('sensor_type', 'unknown')
            location = anomaly.get('location', 'unknown')
            value = anomaly.get('value', 'unknown')
            unit = anomaly.get('unit', '')
            
            return (f"[{severity_str} SEVERITY] Sensor Anomaly Detected\n"
                    f"Sensor: {sensor_id} ({sensor_type})\n"
                    f"Location: {location}\n"
                    f"Value: {value}{unit}\n"
                    f"Anomaly Score: {anomaly_score:.2f}\n"
                    f"Timestamp: {timestamp}")
                    
        elif source == 'social_feed_simulator':
            user_id = anomaly.get('user_id', 'unknown')
            topic = anomaly.get('topic', 'unknown')
            sentiment = anomaly.get('sentiment', 'unknown')
            
            return (f"[{severity_str} SEVERITY] Social Media Anomaly Detected\n"
                    f"User: {user_id}\n"
                    f"Topic: {topic}\n"
                    f"Sentiment: {sentiment}\n"
                    f"Anomaly Score: {anomaly_score:.2f}\n"
                    f"Timestamp: {timestamp}")
                    
        elif source == 'market_data_simulator':
            symbol = anomaly.get('symbol', 'unknown')
            price = anomaly.get('price', 'unknown')
            price_change = anomaly.get('price_change', 'unknown')
            
            return (f"[{severity_str} SEVERITY] Market Anomaly Detected\n"
                    f"Symbol: {symbol}\n"
                    f"Price: {price}\n"
                    f"Price Change: {price_change}\n"
                    f"Anomaly Score: {anomaly_score:.2f}\n"
                    f"Timestamp: {timestamp}")
        
        else:
            # Generic alert for unknown sources
            return (f"[{severity_str} SEVERITY] Anomaly Detected\n"
                    f"Source: {source}\n"
                    f"Details: {json.dumps(anomaly, indent=2)}\n"
                    f"Anomaly Score: {anomaly_score:.2f}\n"
                    f"Timestamp: {timestamp}")
    
    def _send_email_alert(self, alert_msg, anomaly):
        """Send an email alert"""
        if not self.email_enabled or not self.email_recipients:
            return False
            
        try:
            # Create email message
            msg = MIMEMultipart()
            msg['From'] = self.email_sender
            msg['To'] = ', '.join(self.email_recipients)
            msg['Subject'] = f"StreamFlux Alert: {anomaly.get('source', 'Unknown')} Anomaly Detected"
            
            # Add alert message to email
            msg.attach(MIMEText(alert_msg, 'plain'))
            
            # Connect to SMTP server and send email
            server = smtplib.SMTP(self.email_server, self.email_port)
            server.starttls()
            server.login(self.email_sender, self.email_password)
            server.send_message(msg)
            server.quit()
            
            logger.info(f"Email alert sent to {', '.join(self.email_recipients)}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email alert: {str(e)}")
            return False
    
    def _send_webhook_alert(self, alert_msg, anomaly):
        """Send an alert to a webhook endpoint"""
        if not self.webhook_enabled or not self.webhook_url:
            return False
            
        try:
            # Prepare webhook payload
            payload = {
                "text": alert_msg,
                "source": anomaly.get('source', 'unknown'),
                "timestamp": anomaly.get('timestamp', datetime.now().isoformat()),
                "anomaly": anomaly,
                "severity": anomaly.get('anomaly_score', 0)
            }
            
            # Send webhook request
            response = requests.post(
                self.webhook_url,
                json=payload,
                headers={"Content-Type": "application/json"}
            )
            
            if response.status_code >= 200 and response.status_code < 300:
                logger.info(f"Webhook alert sent successfully")
                return True
            else:
                logger.error(f"Webhook alert failed with status {response.status_code}: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"Failed to send webhook alert: {str(e)}")
            return False