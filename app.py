import os
import logging
import tempfile
import zipfile
import shutil
import uuid
from datetime import datetime
from flask import Flask, render_template, request, jsonify, flash, redirect, url_for, session
from werkzeug.utils import secure_filename
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.exc import SQLAlchemyError

# Import our custom modules
from cobol_parser import parse_cobol_to_ast
from ingest import run_ingestion_pipeline
from database_setup import setup_weaviate_schema, get_weaviate_client
from knowledge import build_cobol_knowledge_graph, query_dependencies, search_similar_code, explain_program
from models import db, CobolProgram, AnalysisSession, ChatMessage, ProgramDependency
from database import init_database, store_cobol_program, store_chat_message, update_session_status, get_program_by_id, search_programs_by_text
from llm_integration import llm_service
from analytics_service import analytics_service

# Configure logging with structured format
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

# Use environment variable for secret key with strong default
app.secret_key = os.environ.get("SESSION_SECRET", os.urandom(24).hex())
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Enhanced database configuration
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_size": 5,
    "max_overflow": 10,
    "pool_timeout": 30,
    "pool_recycle": 300,
    "pool_pre_ping": True,
}
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# File upload configuration
UPLOAD_FOLDER = 'uploads'
TEMP_FOLDER = 'temp'
ALLOWED_EXTENSIONS = {'zip', 'cbl', 'cob', 'cobol'}
MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB

app.config.update(
    UPLOAD_FOLDER=UPLOAD_FOLDER,
    TEMP_FOLDER=TEMP_FOLDER,
    MAX_CONTENT_LENGTH=MAX_CONTENT_LENGTH
)

# Ensure directories exist
for folder in [UPLOAD_FOLDER, TEMP_FOLDER]:
    os.makedirs(folder, exist_ok=True)

# Initialize database
db.init_app(app)
with app.app_context():
    init_database(app)

def get_or_create_session():
    """Get or create analysis session with error handling"""
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
        try:
            analysis_session = AnalysisSession(
                session_id=session['session_id'],
                processing_status='ready'
            )
            db.session.add(analysis_session)
            db.session.commit()
        except SQLAlchemyError as e:
            db.session.rollback()
            logger.error(f"Database error creating session: {str(e)}")
            raise
    return session['session_id']

def allowed_file(filename):
    """Validate file extension and basic security checks"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS and \
           not any(c in filename for c in ['..', '/', '\\'])

def _extract_program_name(message):
    """Extract program name from message using regex"""
    import re
    patterns = [
        r'\b([A-Z][A-Z0-9\-]{2,})\b',
        r'program\s+([A-Z0-9\-]+)',
        r'called\s+([A-Z0-9\-]+)'
    ]
    
    message_upper = message.upper()
    for pattern in patterns:
        matches = re.findall(pattern, message_upper)
        if matches:
            return matches[0]
    return None

def _handle_dependency_query(message):
    """Handle dependency-related queries"""
    program_name = _extract_program_name(message)
    
    if program_name:
        program = get_program_by_id(program_name)
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
        response = "Please specify a program name to analyze dependencies."
    
    return response

def _handle_similarity_query(message):
    """Handle similarity search queries"""
    programs = search_programs_by_text(message, limit=5)
    
    if programs:
        response = "Found similar COBOL programs:\n\n"
        for i, program in enumerate(programs, 1):
            response += f"{i}. Program: {program.get('programId', 'Unknown')}\n"
            response += f"   File: {program.get('fileName', 'Unknown')}\n"
            response += f"   Complexity: {program.get('complexity', 'Unknown')}\n"
            
            deps = program.get('dependencies', [])
            if deps:
                response += f"   Dependencies: {', '.join(deps[:3])}"
                if len(deps) > 3:
                    response += f" (and {len(deps) - 3} more)"
                response += "\n"
            response += "\n"
    else:
        response = "No similar COBOL programs found for your query."
    
    return response

def _handle_explanation_query(message):
    """Handle program explanation queries"""
    program_name = _extract_program_name(message)
    
    if program_name:
        program = get_program_by_id(program_name)
        if program:
            response = f"COBOL Program Analysis: {program_name}\n\n"
            response += f"File: {program.get('fileName', 'Unknown')}\n"
            response += f"Complexity: {program.get('complexity', 'Unknown')}\n"
            response += f"Lines of Code: {program.get('lineCount', 0)}\n\n"
            
            procedures = program.get('procedures', [])
            if procedures:
                response += "Procedures/Functions:\n"
                for proc in procedures[:10]:
                    if isinstance(proc, dict):
                        response += f"- {proc.get('name', 'Unknown')}\n"
                    else:
                        response += f"- {proc}\n"
                if len(procedures) > 10:
                    response += f"... and {len(procedures) - 10} more procedures\n"
                response += "\n"
            
            deps = program.get('dependencies', [])
            if deps:
                response += "Dependencies:\n"
                for dep in deps:
                    response += f"- {dep}\n"
        else:
            response = f"Program '{program_name}' not found in the database."
    else:
        programs = search_programs_by_text(message, limit=3)
        if programs:
            response = "Found programs related to your query:\n\n"
            for i, program in enumerate(programs, 1):
                response += f"{i}. {program.get('programId', program.get('fileName', 'Unknown'))}\n"
                response += f"   Complexity: {program.get('complexity', 'Unknown')}\n"
                response += f"   Lines: {program.get('lineCount', 0)}\n\n"
        else:
            response = "No programs found matching your query. Please upload and process COBOL files first."
    
    return response

@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and processing with enhanced security and error handling"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file selected'}), 400
        
        file = request.files['file']
        if not file or not file.filename:
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type or name'}), 400
        
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        
        # Create temporary directory without chmod
        temp_dir = tempfile.mkdtemp(dir=app.config['TEMP_FOLDER'])
        
        try:
            session_id = get_or_create_session()
            update_session_status(session_id, 'processing')
            
            # Save and process file
            file.save(filepath)
            
            if filename.lower().endswith('.zip'):
                with zipfile.ZipFile(filepath, 'r') as zip_ref:
                    # Validate zip contents before extraction
                    for zip_info in zip_ref.infolist():
                        if not allowed_file(zip_info.filename):
                            raise ValueError(f"Invalid file in zip: {zip_info.filename}")
                    zip_ref.extractall(temp_dir)
            else:
                shutil.copy2(filepath, temp_dir)
            
            # Process COBOL files
            from cobol_parser import extract_cobol_files
            cobol_files = extract_cobol_files(temp_dir)
            
            programs_stored = 0
            for file_data in cobol_files:
                program_data = _prepare_program_data(file_data)
                if store_cobol_program(program_data):
                    programs_stored += 1
            
            # Update session and setup services
            update_session_status(session_id, 'completed', programs_stored)
            _setup_external_services(temp_dir)
            
            return jsonify({
                'success': True,
                'message': f'Processing complete. {programs_stored} COBOL programs analyzed and stored.',
                'programs_count': programs_stored
            })
            
        except Exception as e:
            logger.error(f"Processing error: {str(e)}")
            update_session_status(session_id, 'error', error_message=str(e))
            return jsonify({'error': f'Processing failed: {str(e)}'}), 500
            
        finally:
            # Cleanup
            if os.path.exists(filepath):
                os.remove(filepath)
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
                
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

def _prepare_program_data(file_data):
    """Prepare program data for storage"""
    program_data = {
        'program_id': file_data.get('program_id', 'unknown'),
        'file_name': file_data.get('file_name', 'unknown'),
        'file_path': file_data.get('file_path', ''),
        'source_code': '',
        'ast_structure': file_data,
        'procedures': file_data.get('procedures', []),
        'data_divisions': file_data.get('working_storage', []) + file_data.get('file_section', []),
        'dependencies': file_data.get('dependencies', []),
        'copybooks': file_data.get('copybooks', []),
        'business_rules': '',
        'complexity': file_data.get('metadata', {}).get('estimated_complexity', 'Unknown'),
        'line_count': file_data.get('line_count', 0)
    }
    
    if file_data.get('file_path') and os.path.exists(file_data['file_path']):
        try:
            with open(file_data['file_path'], 'r', encoding='utf-8', errors='ignore') as f:
                program_data['source_code'] = f.read()
        except Exception as e:
            logger.error(f"Error reading source: {str(e)}")
    
    return program_data

def _setup_external_services(temp_dir):
    """Setup external services with error handling"""
    try:
        setup_weaviate_schema()
        run_ingestion_pipeline(temp_dir)
        build_cobol_knowledge_graph()
    except Exception as e:
        logger.warning(f"External services setup failed: {str(e)}")

@app.route('/chat', methods=['POST'])
def chat():
    """Handle chat queries with improved error handling and response structure"""
    try:
        data = request.get_json()
        if not data or 'message' not in data:
            return jsonify({'error': 'No message provided'}), 400
        
        message = data['message'].strip()
        if not message:
            return jsonify({'error': 'Empty message'}), 400
        
        session_id = get_or_create_session()
        store_chat_message(session_id, 'user', message)
        
        # Route query to appropriate handler
        query_type, response = _process_chat_query(message)
        
        # Store assistant response
        store_chat_message(session_id, 'assistant', response, query_type)
        
        return jsonify({
            'success': True,
            'response': response,
            'query_type': query_type
        })
        
    except Exception as e:
        logger.error(f"Chat error: {str(e)}")
        return jsonify({'error': f'Query failed: {str(e)}'}), 500

def _process_chat_query(message):
    """Process chat query and determine appropriate handler"""
    message_lower = message.lower()
    
    if any(keyword in message_lower for keyword in ['depend', 'call', 'reference', 'use']):
        return 'dependency', _handle_dependency_query(message)
    elif any(keyword in message_lower for keyword in ['similar', 'like', 'find', 'search']):
        return 'similarity', _handle_similarity_query(message)
    else:
        return 'explanation', _handle_explanation_query(message)

@app.route('/health')
def health_check():
    """Health check endpoint with enhanced database testing"""
    try:
        # Test database connection
        try:
            db.session.execute(db.text('SELECT 1'))
            db_status = 'connected'
        except Exception as db_error:
            logger.error(f"Database connection failed: {str(db_error)}")
            db_status = 'disconnected'
            
        # Check external services
        weaviate_client = get_weaviate_client()
        weaviate_status = 'connected' if weaviate_client and weaviate_client.is_ready() else 'disconnected'
        
        status = {
            'status': 'healthy' if db_status == 'connected' else 'unhealthy',
            'database': db_status,
            'weaviate': weaviate_status,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        return jsonify(status), 200 if status['status'] == 'healthy' else 503
        
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return jsonify({
            'status': 'unhealthy',
            'error': str(e),
            'timestamp': datetime.utcnow().isoformat()
        }), 503

@app.route('/analytics')
def analytics_dashboard():
    """Analytics dashboard page"""
    return render_template('analytics.html')

@app.route('/api/analytics/overview')
def get_analytics_overview():
    """Get codebase analytics overview"""
    try:
        overview = analytics_service.generate_codebase_overview()
        return jsonify(overview)
    except Exception as e:
        logger.error(f"Analytics overview error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/relationships')
def get_relationship_analysis():
    """Get program relationship analysis"""
    try:
        relationships = analytics_service.analyze_program_relationships()
        return jsonify(relationships)
    except Exception as e:
        logger.error(f"Relationship analysis error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/refactoring')
def get_refactoring_opportunities():
    """Get refactoring opportunities"""
    try:
        opportunities = analytics_service.identify_refactoring_opportunities()
        return jsonify(opportunities)
    except Exception as e:
        logger.error(f"Refactoring analysis error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/llm/status')
def get_llm_status():
    """Get LLM provider status"""
    try:
        status = llm_service.get_provider_status()
        return jsonify(status)
    except Exception as e:
        logger.error(f"LLM status error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/llm/configure', methods=['POST'])
def configure_llm():
    """Configure custom LLM endpoint with validation"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No configuration data provided'}), 400
            
        endpoint = data.get('endpoint')
        if not endpoint:
            return jsonify({'error': 'Endpoint URL required'}), 400
            
        # Validate endpoint URL
        from urllib.parse import urlparse
        parsed_url = urlparse(endpoint)
        if not all([parsed_url.scheme, parsed_url.netloc]):
            return jsonify({'error': 'Invalid endpoint URL'}), 400
        
        # Configure LLM
        os.environ['CUSTOM_LLM_ENDPOINT'] = endpoint
        if data.get('token'):
            os.environ['CUSTOM_LLM_TOKEN'] = data.get('token')
        
        # Reinitialize LLM service
        llm_service.initialize_provider()
        
        return jsonify({
            'success': True,
            'message': 'LLM endpoint configured successfully',
            'provider': llm_service.current_provider
        })
            
    except Exception as e:
        logger.error(f"LLM configuration error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.errorhandler(413)
def too_large(e):
    """Handle file too large error"""
    return jsonify({'error': 'File too large. Maximum size is 50MB.'}), 413

@app.errorhandler(500)
def internal_error(e):
    """Handle internal server errors"""
    logger.error(f"Internal error: {str(e)}")
    return jsonify({'error': 'Internal server error. Please try again.'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)