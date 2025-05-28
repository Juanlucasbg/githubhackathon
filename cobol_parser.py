"""
Mock COBOL parser for extracting basic program structure.
This is a placeholder implementation that simulates AST generation
for COBOL programs by identifying basic patterns and structures.
"""

import re
import os
import logging
from typing import Dict, List, Any

def parse_cobol_to_ast(file_content: str, file_path: str = "") -> Dict[str, Any]:
    """
    Mock COBOL parser that extracts basic program structure.
    
    Args:
        file_content: Raw COBOL source code
        file_path: Path to the file being parsed
        
    Returns:
        Dictionary representing a simplified AST structure
    """
    try:
        # Initialize AST structure
        ast = {
            "file_path": file_path,
            "file_name": os.path.basename(file_path) if file_path else "unknown",
            "program_id": "",
            "procedures": [],
            "data_divisions": [],
            "dependencies": [],
            "copybooks": [],
            "working_storage": [],
            "file_section": [],
            "line_count": len(file_content.split('\n'))
        }
        
        # Clean and normalize content
        lines = file_content.split('\n')
        clean_lines = []
        
        for line in lines:
            # Remove comments (starting with *)
            if line.strip().startswith('*'):
                continue
            # Remove line numbers (first 6 characters)
            if len(line) > 6:
                line = line[6:]
            clean_lines.append(line.strip())
        
        content = '\n'.join(clean_lines).upper()
        
        # Extract PROGRAM-ID
        program_id_match = re.search(r'PROGRAM-ID\.\s+([A-Z0-9\-]+)', content)
        if program_id_match:
            ast["program_id"] = program_id_match.group(1)
        
        # Extract procedures/paragraphs
        procedure_patterns = [
            r'([A-Z0-9\-]+)\s+SECTION\.',
            r'^([A-Z0-9\-]+)\.\s*$',
            r'PERFORM\s+([A-Z0-9\-]+)',
        ]
        
        procedures = set()
        for pattern in procedure_patterns:
            matches = re.findall(pattern, content, re.MULTILINE)
            procedures.update(matches)
        
        # Filter out common COBOL keywords that aren't procedures
        cobol_keywords = {
            'IDENTIFICATION', 'ENVIRONMENT', 'DATA', 'PROCEDURE',
            'WORKING-STORAGE', 'FILE', 'LINKAGE', 'LOCAL-STORAGE',
            'CONFIGURATION', 'INPUT-OUTPUT', 'FILE-CONTROL'
        }
        
        ast["procedures"] = [
            {"name": proc, "type": "procedure", "calls": []}
            for proc in procedures if proc not in cobol_keywords
        ]
        
        # Extract COPY statements (dependencies)
        copy_matches = re.findall(r'COPY\s+([A-Z0-9\-]+)', content)
        ast["copybooks"] = list(set(copy_matches))
        ast["dependencies"].extend(copy_matches)
        
        # Extract CALL statements (program dependencies)
        call_matches = re.findall(r'CALL\s+["\']([A-Z0-9\-]+)["\']', content)
        ast["dependencies"].extend(call_matches)
        
        # Extract data divisions
        working_storage_match = re.search(
            r'WORKING-STORAGE\s+SECTION\.(.*?)(?=\w+\s+SECTION\.|PROCEDURE\s+DIVISION|$)', 
            content, re.DOTALL
        )
        if working_storage_match:
            ws_content = working_storage_match.group(1)
            # Extract data items (simplified)
            data_items = re.findall(r'(\d+)\s+([A-Z0-9\-]+)', ws_content)
            ast["working_storage"] = [
                {"level": level, "name": name, "type": "data_item"}
                for level, name in data_items
            ]
        
        # Extract file section
        file_section_match = re.search(
            r'FILE\s+SECTION\.(.*?)(?=\w+\s+SECTION\.|$)', 
            content, re.DOTALL
        )
        if file_section_match:
            fs_content = file_section_match.group(1)
            # Extract FD entries
            fd_matches = re.findall(r'FD\s+([A-Z0-9\-]+)', fs_content)
            ast["file_section"] = [
                {"name": fd, "type": "file_descriptor"}
                for fd in fd_matches
            ]
        
        # Remove duplicates from dependencies
        ast["dependencies"] = list(set(ast["dependencies"]))
        
        # Add some basic metadata
        ast["metadata"] = {
            "has_file_section": bool(ast["file_section"]),
            "has_working_storage": bool(ast["working_storage"]),
            "procedure_count": len(ast["procedures"]),
            "dependency_count": len(ast["dependencies"]),
            "estimated_complexity": _estimate_complexity(content)
        }
        
        logging.debug(f"Parsed COBOL file: {ast['file_name']}, Program ID: {ast['program_id']}")
        return ast
        
    except Exception as e:
        logging.error(f"Error parsing COBOL file {file_path}: {str(e)}")
        return {
            "file_path": file_path,
            "file_name": os.path.basename(file_path) if file_path else "unknown",
            "error": str(e),
            "procedures": [],
            "data_divisions": [],
            "dependencies": [],
            "copybooks": [],
            "working_storage": [],
            "file_section": [],
            "line_count": 0
        }

def _estimate_complexity(content: str) -> str:
    """Estimate code complexity based on various factors"""
    try:
        # Count decision points
        decision_keywords = ['IF', 'WHEN', 'PERFORM', 'UNTIL', 'WHILE']
        decision_count = sum(len(re.findall(keyword, content)) for keyword in decision_keywords)
        
        # Count data structures
        data_count = len(re.findall(r'\d+\s+[A-Z0-9\-]+', content))
        
        # Estimate complexity
        total_score = decision_count + data_count
        
        if total_score < 10:
            return "Low"
        elif total_score < 50:
            return "Medium"
        else:
            return "High"
            
    except Exception:
        return "Unknown"

def extract_cobol_files(directory_path: str) -> List[Dict[str, Any]]:
    """
    Extract and parse all COBOL files from a directory.
    
    Args:
        directory_path: Path to directory containing COBOL files
        
    Returns:
        List of parsed AST dictionaries
    """
    cobol_files = []
    cobol_extensions = {'.cbl', '.cob', '.cobol', '.cpy'}
    
    try:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                _, ext = os.path.splitext(file.lower())
                
                if ext in cobol_extensions:
                    try:
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                        
                        ast = parse_cobol_to_ast(content, file_path)
                        cobol_files.append(ast)
                        
                    except Exception as e:
                        logging.error(f"Error reading file {file_path}: {str(e)}")
                        continue
    
    except Exception as e:
        logging.error(f"Error extracting COBOL files from {directory_path}: {str(e)}")
    
    return cobol_files
