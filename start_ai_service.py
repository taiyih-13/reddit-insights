#!/usr/bin/env python3
"""
Simple script to start AI service and test it works
"""

import os
import sys
import subprocess
import time
import requests
import signal
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def start_service():
    """Start AI service in background"""
    api_key = os.getenv('GROQ_API_KEY')
    
    if not api_key:
        print("❌ Please set GROQ_API_KEY environment variable")
        print("   Example: export GROQ_API_KEY='your-api-key-here'")
        sys.exit(1)
    
    # Set environment variable
    env = os.environ.copy()
    env['GROQ_API_KEY'] = api_key
    
    print("🚀 Starting AI service...")
    
    # Start service
    process = subprocess.Popen(
        [sys.executable, 'ai_summarizer.py'],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    # Wait a moment for service to start
    time.sleep(3)
    
    # Test if it's running
    try:
        response = requests.get('http://localhost:5001/health', timeout=5)
        if response.status_code == 200:
            print("✅ AI service is running!")
            
            # Test summarization
            test_data = {
                "category": "Analysis & Education",
                "time_filter": "weekly"
            }
            
            print("Testing summarization...")
            summary_response = requests.post(
                'http://localhost:5001/summarize', 
                json=test_data,
                timeout=30
            )
            
            if summary_response.status_code == 200:
                result = summary_response.json()
                if result.get('success'):
                    print("✅ Summarization working!")
                    print(f"Summary preview: {result['summary'][:100]}...")
                else:
                    print(f"❌ Summarization failed: {result.get('error')}")
            else:
                print(f"❌ Summarization request failed: {summary_response.status_code}")
                
        else:
            print(f"❌ Service not responding: {response.status_code}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Could not connect to service: {e}")
        
        # Print any error output
        stdout, stderr = process.communicate(timeout=1)
        if stderr:
            print(f"Service errors: {stderr.decode()}")
    
    return process

if __name__ == "__main__":
    process = start_service()
    
    print("\n📝 Service started. To stop it, run: kill", process.pid)
    print("🌐 Dashboard should now work at: http://localhost:5001/summarize")
    
    # Keep process running
    try:
        process.wait()
    except KeyboardInterrupt:
        print("🛑 Stopping service...")
        process.terminate()