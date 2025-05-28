"""
Weaviate database setup and configuration for COBOL program storage.
"""

import os
import logging
import weaviate
from weaviate.classes.config import Configure, Property, DataType
from typing import Optional, Dict, Any, List

def get_weaviate_client() -> Optional[weaviate.Client]:
    """
    Create and return a Weaviate client instance.
    
    Returns:
        Weaviate client or None if connection fails
    """
    try:
        # Get connection details from environment
        weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8080")
        weaviate_api_key = os.getenv("WEAVIATE_API_KEY")
        openai_api_key = os.getenv("OPENAI_API_KEY")
        
        # Configure authentication
        auth_config = None
        if weaviate_api_key:
            auth_config = weaviate.AuthApiKey(api_key=weaviate_api_key)
        
        # Configure additional headers for OpenAI
        additional_headers = {}
        if openai_api_key:
            additional_headers["X-OpenAI-Api-Key"] = openai_api_key
        
        # Create client
        client = weaviate.Client(
            url=weaviate_url,
            auth_client_secret=auth_config,
            additional_headers=additional_headers
        )
        
        # Test connection
        if client.is_ready():
            logging.info("Successfully connected to Weaviate")
            return client
        else:
            logging.error("Weaviate is not ready")
            return None
            
    except Exception as e:
        logging.error(f"Failed to connect to Weaviate: {str(e)}")
        return None

def setup_weaviate_schema() -> bool:
    """
    Setup the Weaviate schema for COBOL programs.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        client = get_weaviate_client()
        if not client:
            return False
        
        # Check if class already exists
        existing_schema = client.schema.get()
        class_names = [cls['class'] for cls in existing_schema.get('classes', [])]
        
        if 'COBOLProgram' in class_names:
            logging.info("COBOLProgram class already exists")
            return True
        
        # Define the COBOLProgram class schema
        cobol_class = {
            "class": "COBOLProgram",
            "description": "A COBOL program with its source code, structure, and metadata",
            "vectorizer": "text2vec-openai",  # Use OpenAI for embeddings
            "moduleConfig": {
                "text2vec-openai": {
                    "model": "ada",
                    "modelVersion": "002",
                    "type": "text"
                }
            },
            "properties": [
                {
                    "name": "fileName",
                    "dataType": ["text"],
                    "description": "Name of the COBOL file"
                },
                {
                    "name": "sourceCode",
                    "dataType": ["text"],
                    "description": "Raw COBOL source code",
                    "moduleConfig": {
                        "text2vec-openai": {
                            "skip": False,
                            "vectorizePropertyName": False
                        }
                    }
                },
                {
                    "name": "astStructure",
                    "dataType": ["object"],
                    "description": "Abstract Syntax Tree structure of the program"
                },
                {
                    "name": "procedures",
                    "dataType": ["object[]"],
                    "description": "List of procedures/paragraphs in the program"
                },
                {
                    "name": "dataDivisions",
                    "dataType": ["object[]"],
                    "description": "Data divisions including working storage and file section"
                },
                {
                    "name": "dependencies",
                    "dataType": ["text[]"],
                    "description": "List of program dependencies (COPY, CALL statements)"
                },
                {
                    "name": "businessRules",
                    "dataType": ["text"],
                    "description": "Extracted business rules and logic description"
                },
                {
                    "name": "programId",
                    "dataType": ["text"],
                    "description": "COBOL PROGRAM-ID"
                },
                {
                    "name": "complexity",
                    "dataType": ["text"],
                    "description": "Estimated complexity level (Low, Medium, High)"
                },
                {
                    "name": "lineCount",
                    "dataType": ["int"],
                    "description": "Number of lines in the source code"
                }
            ]
        }
        
        # Create the class
        client.schema.create_class(cobol_class)
        logging.info("Successfully created COBOLProgram class in Weaviate")
        
        return True
        
    except Exception as e:
        logging.error(f"Failed to setup Weaviate schema: {str(e)}")
        return False

def query_cobol_programs(query: str, limit: int = 10) -> List[Dict[str, Any]]:
    """
    Query COBOL programs using semantic search.
    
    Args:
        query: Search query
        limit: Maximum number of results
        
    Returns:
        List of matching programs
    """
    try:
        client = get_weaviate_client()
        if not client:
            return []
        
        # Perform semantic search
        result = client.query.get("COBOLProgram", [
            "fileName", "sourceCode", "programId", "procedures", 
            "dependencies", "businessRules", "complexity", "lineCount"
        ]).with_near_text({
            "concepts": [query]
        }).with_limit(limit).do()
        
        programs = result.get("data", {}).get("Get", {}).get("COBOLProgram", [])
        return programs
        
    except Exception as e:
        logging.error(f"Error querying COBOL programs: {str(e)}")
        return []

def find_program_by_id(program_id: str) -> Optional[Dict[str, Any]]:
    """
    Find a specific program by its PROGRAM-ID.
    
    Args:
        program_id: The COBOL PROGRAM-ID to search for
        
    Returns:
        Program data or None if not found
    """
    try:
        client = get_weaviate_client()
        if not client:
            return None
        
        # Search by program ID
        result = client.query.get("COBOLProgram", [
            "fileName", "sourceCode", "programId", "procedures", 
            "dependencies", "businessRules", "complexity", "lineCount", "astStructure"
        ]).with_where({
            "path": ["programId"],
            "operator": "Equal",
            "valueText": program_id
        }).do()
        
        programs = result.get("data", {}).get("Get", {}).get("COBOLProgram", [])
        return programs[0] if programs else None
        
    except Exception as e:
        logging.error(f"Error finding program by ID: {str(e)}")
        return None

def find_programs_with_dependencies(dependency: str) -> List[Dict[str, Any]]:
    """
    Find programs that depend on a specific module/program.
    
    Args:
        dependency: Name of the dependency to search for
        
    Returns:
        List of programs that have this dependency
    """
    try:
        client = get_weaviate_client()
        if not client:
            return []
        
        # Search for programs containing the dependency
        result = client.query.get("COBOLProgram", [
            "fileName", "sourceCode", "programId", "procedures", 
            "dependencies", "businessRules", "complexity"
        ]).with_where({
            "path": ["dependencies"],
            "operator": "ContainsAny",
            "valueText": [dependency]
        }).do()
        
        programs = result.get("data", {}).get("Get", {}).get("COBOLProgram", [])
        return programs
        
    except Exception as e:
        logging.error(f"Error finding programs with dependencies: {str(e)}")
        return []

def get_all_programs() -> List[Dict[str, Any]]:
    """
    Get all COBOL programs from the database.
    
    Returns:
        List of all programs
    """
    try:
        client = get_weaviate_client()
        if not client:
            return []
        
        # Get all programs
        result = client.query.get("COBOLProgram", [
            "fileName", "programId", "dependencies", "complexity", "lineCount"
        ]).do()
        
        programs = result.get("data", {}).get("Get", {}).get("COBOLProgram", [])
        return programs
        
    except Exception as e:
        logging.error(f"Error getting all programs: {str(e)}")
        return []

def delete_all_programs() -> bool:
    """
    Delete all COBOL programs from the database.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        client = get_weaviate_client()
        if not client:
            return False
        
        # Delete all objects of COBOLProgram class
        client.batch.delete_objects(
            class_name="COBOLProgram",
            where={
                "path": ["fileName"],
                "operator": "Like",
                "valueText": "*"
            }
        )
        
        logging.info("Successfully deleted all COBOL programs")
        return True
        
    except Exception as e:
        logging.error(f"Error deleting programs: {str(e)}")
        return False
