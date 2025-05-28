"""
Knowledge graph creation and querying using Cognee.ai
for understanding COBOL program relationships and dependencies.
"""

import os
import logging
import re
from typing import Dict, Any, List, Optional
import cognee
from database_setup import get_weaviate_client, get_all_programs, find_program_by_id, query_cobol_programs

# Initialize Cognee
def initialize_cognee():
    """Initialize Cognee with configuration"""
    try:
        # Set up Cognee configuration
        cognee_api_key = os.getenv("COGNEE_API_KEY")
        if cognee_api_key:
            cognee.config.set("COGNEE_API_KEY", cognee_api_key)
        
        # Set OpenAI API key for LLM operations
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if openai_api_key:
            cognee.config.set("OPENAI_API_KEY", openai_api_key)
        
        logging.info("Cognee initialized successfully")
        return True
    except Exception as e:
        logging.error(f"Failed to initialize Cognee: {str(e)}")
        return False

def build_cobol_knowledge_graph() -> bool:
    """
    Build knowledge graph from COBOL programs stored in Weaviate.
    
    Returns:
        True if successful, False otherwise
    """
    try:
        logging.info("Building COBOL knowledge graph...")
        
        # Initialize Cognee
        if not initialize_cognee():
            return False
        
        # Get all programs from Weaviate
        programs = get_all_programs()
        if not programs:
            logging.warning("No COBOL programs found in database")
            return False
        
        # Prepare data for knowledge graph
        knowledge_data = []
        
        for program in programs:
            # Create program description
            program_description = f"""
            COBOL Program: {program.get('programId', 'Unknown')}
            File: {program.get('fileName', 'Unknown')}
            Complexity: {program.get('complexity', 'Unknown')}
            Dependencies: {', '.join(program.get('dependencies', []))}
            Line Count: {program.get('lineCount', 0)}
            """
            
            knowledge_data.append({
                "id": program.get('programId', program.get('fileName', 'unknown')),
                "type": "COBOL_PROGRAM",
                "content": program_description,
                "metadata": {
                    "fileName": program.get('fileName'),
                    "programId": program.get('programId'),
                    "dependencies": program.get('dependencies', []),
                    "complexity": program.get('complexity')
                }
            })
        
        # Add relationship data
        relationship_data = _extract_relationships(programs)
        knowledge_data.extend(relationship_data)
        
        # Add data to Cognee
        for data in knowledge_data:
            cognee.add_data(data)
        
        # Build the knowledge graph
        cognee.cognify()
        
        logging.info(f"Successfully built knowledge graph with {len(programs)} programs")
        return True
        
    except Exception as e:
        logging.error(f"Failed to build knowledge graph: {str(e)}")
        return False

def _extract_relationships(programs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Extract relationships between COBOL programs"""
    relationships = []
    
    try:
        for program in programs:
            program_id = program.get('programId', '')
            dependencies = program.get('dependencies', [])
            
            for dep in dependencies:
                relationships.append({
                    "id": f"{program_id}_DEPENDS_ON_{dep}",
                    "type": "RELATIONSHIP",
                    "content": f"Program {program_id} depends on {dep}",
                    "metadata": {
                        "source": program_id,
                        "target": dep,
                        "relationship_type": "DEPENDS_ON"
                    }
                })
        
        return relationships
        
    except Exception as e:
        logging.error(f"Error extracting relationships: {str(e)}")
        return []

def query_dependencies(query: str) -> str:
    """
    Query program dependencies using the knowledge graph.
    
    Args:
        query: User query about dependencies
        
    Returns:
        Formatted response about dependencies
    """
    try:
        # Extract program name from query
        program_name = _extract_program_name_from_query(query)
        
        if program_name:
            # Get specific program info
            program = find_program_by_id(program_name)
            if program:
                dependencies = program.get('dependencies', [])
                if dependencies:
                    response = f"Program '{program_name}' depends on:\n"
                    for i, dep in enumerate(dependencies, 1):
                        response += f"{i}. {dep}\n"
                else:
                    response = f"Program '{program_name}' has no external dependencies."
            else:
                response = f"Program '{program_name}' not found in the database."
        else:
            # General dependency query using Cognee
            try:
                cognee_response = cognee.search(query)
                if cognee_response:
                    response = f"Dependency Analysis:\n{cognee_response}"
                else:
                    response = "No dependency information found for this query."
            except Exception:
                # Fallback to Weaviate search
                programs = query_cobol_programs(query, limit=5)
                if programs:
                    response = "Found related programs:\n"
                    for i, prog in enumerate(programs, 1):
                        deps = prog.get('dependencies', [])
                        response += f"{i}. {prog.get('programId', prog.get('fileName', 'Unknown'))}"
                        if deps:
                            response += f" - Dependencies: {', '.join(deps)}\n"
                        else:
                            response += " - No dependencies\n"
                else:
                    response = "No programs found matching your query."
        
        return response
        
    except Exception as e:
        logging.error(f"Error querying dependencies: {str(e)}")
        return f"Error processing dependency query: {str(e)}"

def search_similar_code(query: str) -> str:
    """
    Search for similar code using semantic search.
    
    Args:
        query: User query for similar code
        
    Returns:
        Formatted response with similar programs
    """
    try:
        # Use Weaviate's semantic search
        programs = query_cobol_programs(query, limit=5)
        
        if programs:
            response = "Found similar COBOL programs:\n\n"
            for i, program in enumerate(programs, 1):
                response += f"{i}. Program: {program.get('programId', 'Unknown')}\n"
                response += f"   File: {program.get('fileName', 'Unknown')}\n"
                response += f"   Complexity: {program.get('complexity', 'Unknown')}\n"
                
                # Show dependencies if any
                deps = program.get('dependencies', [])
                if deps:
                    response += f"   Dependencies: {', '.join(deps[:3])}"
                    if len(deps) > 3:
                        response += f" (and {len(deps) - 3} more)"
                    response += "\n"
                
                # Show snippet of source code if available
                source = program.get('sourceCode', '')
                if source:
                    lines = source.split('\n')[:5]  # First 5 lines
                    snippet = '\n'.join(lines)
                    response += f"   Code snippet:\n   {snippet}\n"
                
                response += "\n"
        else:
            response = "No similar COBOL programs found for your query."
        
        return response
        
    except Exception as e:
        logging.error(f"Error searching similar code: {str(e)}")
        return f"Error searching for similar code: {str(e)}"

def explain_program(query: str) -> str:
    """
    Explain a COBOL program's functionality.
    
    Args:
        query: User query asking for program explanation
        
    Returns:
        Formatted explanation of the program
    """
    try:
        # Extract program name from query
        program_name = _extract_program_name_from_query(query)
        
        if program_name:
            # Get specific program
            program = find_program_by_id(program_name)
            if program:
                response = f"COBOL Program Analysis: {program_name}\n\n"
                response += f"File: {program.get('fileName', 'Unknown')}\n"
                response += f"Complexity: {program.get('complexity', 'Unknown')}\n"
                response += f"Lines of Code: {program.get('lineCount', 0)}\n\n"
                
                # Show procedures
                procedures = program.get('procedures', [])
                if procedures:
                    response += "Procedures/Functions:\n"
                    for proc in procedures[:10]:  # Limit to first 10
                        if isinstance(proc, dict):
                            response += f"- {proc.get('name', 'Unknown')}\n"
                        else:
                            response += f"- {proc}\n"
                    if len(procedures) > 10:
                        response += f"... and {len(procedures) - 10} more procedures\n"
                    response += "\n"
                
                # Show dependencies
                deps = program.get('dependencies', [])
                if deps:
                    response += f"Dependencies:\n"
                    for dep in deps:
                        response += f"- {dep}\n"
                    response += "\n"
                
                # Show AST structure if available
                ast = program.get('astStructure', {})
                if ast and isinstance(ast, dict):
                    metadata = ast.get('metadata', {})
                    if metadata:
                        response += "Program Structure:\n"
                        response += f"- Has File Section: {metadata.get('has_file_section', False)}\n"
                        response += f"- Has Working Storage: {metadata.get('has_working_storage', False)}\n"
                        response += f"- Procedure Count: {metadata.get('procedure_count', 0)}\n"
                        response += f"- Dependency Count: {metadata.get('dependency_count', 0)}\n"
                
            else:
                response = f"Program '{program_name}' not found in the database."
        else:
            # General explanation query
            programs = query_cobol_programs(query, limit=3)
            if programs:
                response = "Found programs related to your query:\n\n"
                for i, program in enumerate(programs, 1):
                    response += f"{i}. {program.get('programId', program.get('fileName', 'Unknown'))}\n"
                    response += f"   Complexity: {program.get('complexity', 'Unknown')}\n"
                    response += f"   Lines: {program.get('lineCount', 0)}\n\n"
            else:
                response = "No programs found matching your query."
        
        return response
        
    except Exception as e:
        logging.error(f"Error explaining program: {str(e)}")
        return f"Error explaining program: {str(e)}"

def _extract_program_name_from_query(query: str) -> Optional[str]:
    """Extract program name from user query"""
    try:
        # Look for patterns like "PROGRAM-A", "program ABC", etc.
        patterns = [
            r'\b([A-Z][A-Z0-9\-]{2,})\b',  # PROGRAM-NAME pattern
            r'program\s+([A-Z0-9\-]+)',     # "program NAME" pattern
            r'called\s+([A-Z0-9\-]+)',      # "called NAME" pattern
        ]
        
        query_upper = query.upper()
        for pattern in patterns:
            matches = re.findall(pattern, query_upper)
            if matches:
                # Filter out common COBOL keywords
                keywords = {'PROGRAM', 'DIVISION', 'SECTION', 'PROCEDURE', 'DATA', 'WORKING', 'STORAGE'}
                for match in matches:
                    if match not in keywords:
                        return match
        
        return None
        
    except Exception as e:
        logging.error(f"Error extracting program name: {str(e)}")
        return None

def get_knowledge_graph_stats() -> Dict[str, Any]:
    """Get statistics about the knowledge graph"""
    try:
        programs = get_all_programs()
        
        # Calculate statistics
        total_programs = len(programs)
        total_dependencies = sum(len(p.get('dependencies', [])) for p in programs)
        complexity_stats = {}
        
        for program in programs:
            complexity = program.get('complexity', 'Unknown')
            complexity_stats[complexity] = complexity_stats.get(complexity, 0) + 1
        
        return {
            "total_programs": total_programs,
            "total_dependencies": total_dependencies,
            "complexity_distribution": complexity_stats,
            "average_dependencies": total_dependencies / total_programs if total_programs > 0 else 0
        }
        
    except Exception as e:
        logging.error(f"Error getting knowledge graph stats: {str(e)}")
        return {"error": str(e)}
