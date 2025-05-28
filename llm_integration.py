"""
LLM Integration Service for COBOL CodeSense
Supports multiple LLM providers including FriendliAI, OpenAI, and custom endpoints
"""

import os
import logging
from typing import Dict, Any, Optional, List
import requests
import json
from datetime import datetime

try:
    import litellm
    LITELLM_AVAILABLE = True
except ImportError:
    LITELLM_AVAILABLE = False

class LLMService:
    """Unified LLM service supporting multiple providers"""
    
    def __init__(self):
        self.current_provider = None
        self.custom_endpoint = None
        self.api_key = None
        self.initialize_provider()
    
    def initialize_provider(self):
        """Initialize the best available LLM provider"""
        # Check for custom endpoint first (highest priority)
        custom_endpoint = os.getenv("CUSTOM_LLM_ENDPOINT")
        if custom_endpoint:
            self.current_provider = "custom"
            self.custom_endpoint = custom_endpoint
            logging.info("Using custom LLM endpoint")
            return
        
        # Check for FriendliAI
        friendli_token = os.getenv("FRIENDLI_TOKEN")
        if friendli_token:
            self.current_provider = "friendli"
            self.api_key = friendli_token
            logging.info("Using FriendliAI provider")
            return
        
        # Check for OpenAI
        openai_key = os.getenv("OPENAI_API_KEY")
        if openai_key and LITELLM_AVAILABLE:
            self.current_provider = "openai"
            self.api_key = openai_key
            logging.info("Using OpenAI provider")
            return
        
        # Fallback to local processing
        self.current_provider = "local"
        logging.info("Using local processing (no external LLM)")
    
    def generate_response(self, prompt: str, context: str = "", max_tokens: int = 500) -> str:
        """Generate response using the configured LLM provider"""
        try:
            if self.current_provider == "custom":
                return self._call_custom_endpoint(prompt, context, max_tokens)
            elif self.current_provider == "friendli":
                return self._call_friendli(prompt, context, max_tokens)
            elif self.current_provider == "openai":
                return self._call_openai(prompt, context, max_tokens)
            else:
                return self._local_processing(prompt, context)
        except Exception as e:
            logging.error(f"LLM generation failed: {str(e)}")
            return self._local_processing(prompt, context)
    
    def _call_custom_endpoint(self, prompt: str, context: str, max_tokens: int) -> str:
        """Call custom LLM endpoint"""
        try:
            payload = {
                "messages": [
                    {"role": "system", "content": "You are a COBOL expert assistant helping analyze legacy code."},
                    {"role": "user", "content": f"Context: {context}\n\nQuestion: {prompt}"}
                ],
                "max_tokens": max_tokens,
                "temperature": 0.7
            }
            
            headers = {
                "Content-Type": "application/json"
            }
            
            # Add authentication if available
            auth_token = os.getenv("CUSTOM_LLM_TOKEN")
            if auth_token:
                headers["Authorization"] = f"Bearer {auth_token}"
            
            response = requests.post(
                self.custom_endpoint,
                json=payload,
                headers=headers,
                timeout=30
            )
            
            if response.status_code == 200:
                result = response.json()
                # Handle different response formats
                if "choices" in result and len(result["choices"]) > 0:
                    return result["choices"][0]["message"]["content"]
                elif "response" in result:
                    return result["response"]
                elif "text" in result:
                    return result["text"]
                else:
                    return str(result)
            else:
                logging.error(f"Custom endpoint error: {response.status_code}")
                return self._local_processing(prompt, context)
                
        except Exception as e:
            logging.error(f"Custom endpoint call failed: {str(e)}")
            return self._local_processing(prompt, context)
    
    def _call_friendli(self, prompt: str, context: str, max_tokens: int) -> str:
        """Call FriendliAI API"""
        try:
            # FriendliAI API call
            url = "https://inference.friendli.ai/v1/chat/completions"
            
            payload = {
                "model": "meta-llama-3.1-8b-instruct",
                "messages": [
                    {"role": "system", "content": "You are a COBOL expert assistant helping analyze legacy code."},
                    {"role": "user", "content": f"Context: {context}\n\nQuestion: {prompt}"}
                ],
                "max_tokens": max_tokens,
                "temperature": 0.7
            }
            
            headers = {
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json"
            }
            
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            
            if response.status_code == 200:
                result = response.json()
                return result["choices"][0]["message"]["content"]
            else:
                logging.error(f"FriendliAI error: {response.status_code}")
                return self._local_processing(prompt, context)
                
        except Exception as e:
            logging.error(f"FriendliAI call failed: {str(e)}")
            return self._local_processing(prompt, context)
    
    def _call_openai(self, prompt: str, context: str, max_tokens: int) -> str:
        """Call OpenAI API using litellm"""
        try:
            if not LITELLM_AVAILABLE:
                return self._local_processing(prompt, context)
            
            messages = [
                {"role": "system", "content": "You are a COBOL expert assistant helping analyze legacy code."},
                {"role": "user", "content": f"Context: {context}\n\nQuestion: {prompt}"}
            ]
            
            response = litellm.completion(
                model="gpt-3.5-turbo",
                messages=messages,
                max_tokens=max_tokens,
                temperature=0.7
            )
            
            return response.choices[0].message.content
            
        except Exception as e:
            logging.error(f"OpenAI call failed: {str(e)}")
            return self._local_processing(prompt, context)
    
    def _local_processing(self, prompt: str, context: str) -> str:
        """Local processing fallback when no LLM is available"""
        # Analyze the prompt and provide structured responses
        prompt_lower = prompt.lower()
        
        if "depend" in prompt_lower:
            return "To analyze dependencies, I need the specific program name. Please specify which COBOL program you'd like me to analyze for dependencies."
        
        elif "explain" in prompt_lower:
            if context:
                return f"Based on the available data, this appears to be a COBOL program with the following characteristics:\n\n{context}\n\nFor a more detailed explanation, please provide the specific program name or code section you'd like me to analyze."
            else:
                return "To explain a COBOL program, please specify which program you'd like me to analyze, or upload your COBOL files first."
        
        elif "similar" in prompt_lower or "find" in prompt_lower:
            return "To find similar code, I can search through your uploaded COBOL programs. Please specify what type of functionality or pattern you're looking for."
        
        else:
            return "I can help you analyze COBOL code! I can:\n\n• Find program dependencies\n• Explain what programs do\n• Search for similar code patterns\n• Analyze program structure\n\nPlease upload your COBOL files first, then ask me specific questions about your code."
    
    def analyze_cobol_program(self, program_data: Dict[str, Any]) -> str:
        """Generate comprehensive analysis of a COBOL program"""
        program_id = program_data.get('programId', 'Unknown')
        complexity = program_data.get('complexity', 'Unknown')
        procedures = program_data.get('procedures', [])
        dependencies = program_data.get('dependencies', [])
        line_count = program_data.get('lineCount', 0)
        
        context = f"""
        Program ID: {program_id}
        Complexity: {complexity}
        Lines of Code: {line_count}
        Number of Procedures: {len(procedures)}
        Dependencies: {', '.join(dependencies) if dependencies else 'None'}
        """
        
        prompt = f"Provide a comprehensive analysis of this COBOL program including its purpose, structure, and any potential issues or improvements."
        
        return self.generate_response(prompt, context, max_tokens=800)
    
    def get_provider_status(self) -> Dict[str, Any]:
        """Get current provider status and configuration"""
        return {
            "current_provider": self.current_provider,
            "custom_endpoint": self.custom_endpoint,
            "has_api_key": bool(self.api_key),
            "litellm_available": LITELLM_AVAILABLE,
            "available_providers": self._get_available_providers()
        }
    
    def _get_available_providers(self) -> List[str]:
        """Get list of available LLM providers"""
        providers = ["local"]
        
        if os.getenv("CUSTOM_LLM_ENDPOINT"):
            providers.append("custom")
        
        if os.getenv("FRIENDLI_TOKEN"):
            providers.append("friendli")
        
        if os.getenv("OPENAI_API_KEY") and LITELLM_AVAILABLE:
            providers.append("openai")
        
        return providers

# Global LLM service instance
llm_service = LLMService()