# data_producers/social_feed_simulator.py
import json
import random
import time
from datetime import datetime
import uuid
import sys
import os

# Add root directory to path to import kafka_layer module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from kafka_layer.producer import KafkaProducer

class SocialFeedSimulator:
    """Simulates social media data streams"""
    
    def __init__(self, num_users=10, kafka_topic="social-data"):
        self.num_users = num_users
        self.kafka_topic = kafka_topic
        self.producer = KafkaProducer()
        self.users = self._initialize_users()
        
        # Load sample content for social posts
        self.topics = ["tech", "weather", "sports", "news", "entertainment"]
        self.sentiments = ["positive", "neutral", "negative"]
        
        # Sample words for each sentiment and topic
        self.word_bank = {
            "positive": ["great", "awesome", "excellent", "amazing", "good", "love", "happy", "perfect", "best", "wonderful"],
            "neutral": ["okay", "fine", "regular", "normal", "average", "typical", "standard", "common", "usual", "moderate"],
            "negative": ["bad", "terrible", "awful", "poor", "horrible", "hate", "dislike", "worst", "disappointing", "failure"]
        }
        
        self.topic_words = {
            "tech": ["computer", "software", "app", "technology", "digital", "innovation", "AI", "smartphone", "coding", "device"],
            "weather": ["sunny", "cloudy", "rainy", "storm", "forecast", "temperature", "humidity", "climate", "wind", "weather"],
            "sports": ["game", "team", "player", "score", "win", "lose", "championship", "tournament", "match", "athlete"],
            "news": ["report", "headline", "story", "event", "update", "politics", "economy", "announcement", "breaking", "media"],
            "entertainment": ["movie", "music", "celebrity", "show", "stream", "concert", "performance", "actor", "film", "song"]
        }
        
    def _initialize_users(self):
        """Create a set of simulated social media users"""
        users = []
        for i in range(self.num_users):
            user_id = f"user-{uuid.uuid4().hex[:8]}"
            users.append({
                "user_id": user_id,
                "post_frequency": random.uniform(0.1, 1.0),  # Posts per second on average
                "preferred_topics": random.sample(["tech", "weather", "sports", "news", "entertainment"], 
                                                 k=random.randint(1, 3)),
                "sentiment_bias": random.choice(["positive", "neutral", "negative"]),
                "anomaly_probability": 0.03  # 3% chance of generating anomalous data
            })
        return users
    
    def _generate_post_content(self, user):
        """Generate simulated post content based on user preferences"""
        topic = random.choice(user["preferred_topics"])
        
        # Bias sentiment based on user's typical sentiment
        sentiment_weights = {
            "positive": {"positive": 0.7, "neutral": 0.2, "negative": 0.1},
            "neutral": {"positive": 0.3, "neutral": 0.4, "negative": 0.3},
            "negative": {"positive": 0.1, "neutral": 0.2, "negative": 0.7}
        }
        
        weights = sentiment_weights[user["sentiment_bias"]]
        sentiment = random.choices(
            ["positive", "neutral", "negative"],
            [weights["positive"], weights["neutral"], weights["negative"]]
        )[0]
        
        # Generate a simple post by combining words from the topic and sentiment
        topic_word_count = random.randint(3, 8)
        sentiment_word_count = random.randint(2, 5)
        
        topic_words = random.sample(self.topic_words[topic], min(topic_word_count, len(self.topic_words[topic])))
        sentiment_words = random.sample(self.word_bank[sentiment], min(sentiment_word_count, len(self.word_bank[sentiment])))
        
        all_words = topic_words + sentiment_words
        random.shuffle(all_words)
        
        # Add some common words to make it more readable
        common_words = ["the", "is", "are", "was", "a", "an", "this", "that", "and", "or", "but", "to", "with", "for"]
        for i in range(len(all_words) - 1, 0, -1):
            if random.random() < 0.3:  # 30% chance to insert a common word
                all_words.insert(i, random.choice(common_words))
        
        post = " ".join(all_words)
        # Capitalize first letter and add end punctuation
        post = post[0].upper() + post[1:] + random.choice([".", "!", "?"])
        
        return {
            "content": post,
            "topic": topic,
            "sentiment": sentiment
        }
    
    def generate_social_post(self, user):
        """Generate a single social media post"""
        timestamp = datetime.now().isoformat()
        anomaly = random.random() < user["anomaly_probability"]
        
        # Generate post content
        post_data = self._generate_post_content(user)
        
        # For anomalies, switch the sentiment to its opposite
        if anomaly:
            if post_data["sentiment"] == "positive":
                post_data["sentiment"] = "negative"
            elif post_data["sentiment"] == "negative":
                post_data["sentiment"] = "positive"
            
            # Also make the content more extreme
            extreme_words = {
                "positive": ["ecstatic", "phenomenal", "extraordinary", "revolutionary", "mind-blowing"],
                "negative": ["catastrophic", "disastrous", "horrific", "atrocious", "despicable"]
            }
            
            if post_data["sentiment"] in extreme_words:
                # Add extreme words to the content
                words = post_data["content"].split()
                for _ in range(min(2, len(words))):
                    i = random.randint(0, len(words) - 1)
                    words[i] = random.choice(extreme_words[post_data["sentiment"]])
                post_data["content"] = " ".join(words)
        
        return {
            "user_id": user["user_id"],
            "timestamp": timestamp,
            "content": post_data["content"],
            "topic": post_data["topic"],
            "sentiment": post_data["sentiment"],
            "engagement_count": random.randint(0, 1000),
            "is_anomaly": anomaly,
            "source": "social_feed_simulator"
        }
    
    def run(self, interval=0.1, duration=None):
        """
        Run the simulator to generate and send social media data.
        
        Args:
            interval: Minimum time between posts in seconds
            duration: How long to run in seconds, None for indefinite
        """
        start_time = time.time()
        count = 0
        
        try:
            print(f"Starting social feed simulator with {self.num_users} users...")
            while duration is None or time.time() - start_time < duration:
                for user in self.users:
                    # Only generate a post based on the user's posting frequency
                    if random.random() < user["post_frequency"] * interval:
                        post = self.generate_social_post(user)
                        
                        # Send to Kafka
                        self.producer.send(self.kafka_topic, post)
                        
                        # Print sample data occasionally
                        if count % 50 == 0:
                            print(f"Sample social post: {json.dumps(post, indent=2)}")
                        
                        count += 1
                
                time.sleep(interval)
                
        except KeyboardInterrupt:
            print("Social feed simulator stopped by user")
        finally:
            self.producer.close()
            print(f"Social feed simulator finished after sending {count} posts")