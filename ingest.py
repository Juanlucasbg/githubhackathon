"""
Data ingestion pipeline using DLT Hub to process COBOL files
and load them into Weaviate vector database.
"""

import os
import logging
import tempfile
from typing import Iterator, Dict, Any, List
import dlt
from cobol_parser import extract_cobol_files, parse_cobol_to_ast
from database_setup import get_weaviate_client

def cobol_source(files_path: str) -> Iterator[Dict[str, Any]]:
    """
    DLT source function that processes COBOL files and yields data.
    
    Args:
        files_path: Path to directory containing COBOL files
        
    Yields:
        Dictionary containing processed COBOL file data
    """
    try:
        logging.info(f"Processing COBOL files from: {files_path}")
        
        # Extract and parse all COBOL files
        cobol_files = extract_cobol_files(files_path)
        
        for file_data in cobol_files:
            # Read raw source code
            source_code = ""
            if os.path.exists(file_data.get("file_path", "")):
                try:
                    with open(file_data["file_path"], 'r', encoding='utf-8', errors='ignore') as f:
                        source_code = f.read()
                except Exception as e:
                    logging.error(f"Error reading source for {file_data['file_path']}: {str(e)}")
            
            # Prepare data for DLT pipeline
            yield {
                "file_name": file_data.get("file_name", "unknown"),
                "file_path": file_data.get("file_path", ""),
                "source_code": source_code,
                "ast_structure": file_data,
                "program_id": file_data.get("program_id", ""),
                "procedures": file_data.get("procedures", []),
                "data_divisions": file_data.get("working_storage", []) + file_data.get("file_section", []),
                "dependencies": file_data.get("dependencies", []),
                "copybooks": file_data.get("copybooks", []),
                "business_rules": "",  # To be populated later through analysis
                "metadata": file_data.get("metadata", {}),
                "line_count": file_data.get("line_count", 0),
                "complexity": file_data.get("metadata", {}).get("estimated_complexity", "Unknown")
            }
            
    except Exception as e:
        logging.error(f"Error in cobol_source: {str(e)}")
        raise

@dlt.resource(write_disposition="replace")
def cobol_programs(files_path: str):
    """DLT resource for COBOL programs"""
    return cobol_source(files_path)

def run_ingestion_pipeline(files_path: str) -> bool:
    """
    Run the complete ingestion pipeline to process COBOL files
    and load them into Weaviate.
    
    Args:
        files_path: Path to directory containing COBOL files
        
    Returns:
        True if successful, False otherwise
    """
    try:
        logging.info("Starting COBOL ingestion pipeline...")
        
        # Get Weaviate connection details
        weaviate_url = os.getenv("WEAVIATE_URL", "http://localhost:8080")
        weaviate_api_key = os.getenv("WEAVIATE_API_KEY")
        
        # Configure DLT pipeline
        pipeline = dlt.pipeline(
            pipeline_name="cobol_analysis",
            destination="weaviate",
            dataset_name="cobol_programs"
        )
        
        # Configure Weaviate destination
        weaviate_config = {
            "weaviate_url": weaviate_url,
            "batch_size": 100,
            "batch_timeout": 30
        }
        
        if weaviate_api_key:
            weaviate_config["api_key"] = weaviate_api_key
        
        # Run the pipeline
        info = pipeline.run(
            cobol_programs(files_path),
            destination=dlt.destinations.weaviate(**weaviate_config)
        )
        
        logging.info(f"Pipeline completed successfully: {info}")
        
        # Also load data directly into Weaviate for immediate querying
        _load_to_weaviate_directly(files_path)
        
        return True
        
    except Exception as e:
        logging.error(f"Ingestion pipeline failed: {str(e)}")
        return False

def _load_to_weaviate_directly(files_path: str):
    """
    Load data directly to Weaviate for immediate querying.
    This supplements the DLT pipeline for real-time access.
    """
    try:
        client = get_weaviate_client()
        if not client or not client.is_ready():
            logging.error("Weaviate client not ready")
            return
        
        # Process COBOL files
        cobol_files = extract_cobol_files(files_path)
        
        # Batch upload to Weaviate
        with client.batch as batch:
            for file_data in cobol_files:
                # Read source code
                source_code = ""
                if os.path.exists(file_data.get("file_path", "")):
                    try:
                        with open(file_data["file_path"], 'r', encoding='utf-8', errors='ignore') as f:
                            source_code = f.read()
                    except Exception as e:
                        logging.error(f"Error reading source: {str(e)}")
                
                # Prepare object for Weaviate
                properties = {
                    "fileName": file_data.get("file_name", "unknown"),
                    "sourceCode": source_code,
                    "astStructure": file_data,
                    "procedures": file_data.get("procedures", []),
                    "dataDivisions": file_data.get("working_storage", []) + file_data.get("file_section", []),
                    "dependencies": file_data.get("dependencies", []),
                    "businessRules": "",
                    "programId": file_data.get("program_id", ""),
                    "complexity": file_data.get("metadata", {}).get("estimated_complexity", "Unknown"),
                    "lineCount": file_data.get("line_count", 0)
                }
                
                # Add to batch
                batch.add_data_object(
                    data_object=properties,
                    class_name="COBOLProgram"
                )
        
        logging.info(f"Successfully loaded {len(cobol_files)} COBOL programs to Weaviate")
        
    except Exception as e:
        logging.error(f"Error loading data directly to Weaviate: {str(e)}")

def get_pipeline_status() -> Dict[str, Any]:
    """Get status of the last pipeline run"""
    try:
        pipeline = dlt.pipeline(
            pipeline_name="cobol_analysis",
            destination="weaviate",
            dataset_name="cobol_programs"
        )
        
        # Get pipeline state
        state = pipeline.state
        
        return {
            "pipeline_name": "cobol_analysis",
            "last_run": state.get("last_run_time", "Never"),
            "status": "healthy" if state else "not_initialized"
        }
        
    except Exception as e:
        logging.error(f"Error getting pipeline status: {str(e)}")
        return {
            "pipeline_name": "cobol_analysis", 
            "status": "error",
            "error": str(e)
        }
