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

# Import our custom modules
from cobol_parser import parse_cobol_to_ast
from ingest import run_ingestion_pipeline
from database_setup import setup_weaviate_schema, get_weaviate_client
from knowledge import build_cobol_knowledge_graph, query_dependencies, search_similar_code, explain_program

# Import database modules
from models import db, CobolProgram, AnalysisSession, ChatMessage, ProgramDependency
from database import init_database, store_cobol_program, store_chat_message, update_session_status
from llm_integration import llm_service
from analytics_service import analytics_service

# Configure logging
logging.basicConfig(level=logging.DEBUG)

# Create Flask app
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev-secret-key-change-in-production")
app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)

# Configure the database
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

# Initialize the database
db.init_app(app)

# Configuration
UPLOAD_FOLDER = 'uploads'
TEMP_FOLDER = 'temp'
ALLOWED_EXTENSIONS = {'zip', 'cbl', 'cob', 'cobol'}

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['TEMP_FOLDER'] = TEMP_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 50 * 1024 * 1024  # 50MB max file size

# Ensure directories exist
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(TEMP_FOLDER, exist_ok=True)

# Initialize database
with app.app_context():
    init_database(app)

def get_or_create_session():
    """Get or create analysis session"""
    if 'session_id' not in session:
        session['session_id'] = str(uuid.uuid4())
        
        # Create new analysis session in database
        try:
            analysis_session = AnalysisSession()
            analysis_session.session_id = session['session_id']
            analysis_session.processing_status = 'ready'
            db.session.add(analysis_session)
            db.session.commit()
        except Exception as e:
            logging.error(f"Error creating session: {str(e)}")
    
    return session['session_id']

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/')
def index():
    """Main page"""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload and processing"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file selected'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if file and file.filename and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(filepath)
            
            # Create temporary directory for extraction
            temp_dir = tempfile.mkdtemp(dir=app.config['TEMP_FOLDER'])
            
            try:
                session_id = get_or_create_session()
                update_session_status(session_id, 'processing')
                
                # Extract ZIP file or process single file
                if filename.lower().endswith('.zip'):
                    with zipfile.ZipFile(filepath, 'r') as zip_ref:
                        zip_ref.extractall(temp_dir)
                else:
                    # Single COBOL file
                    shutil.copy2(filepath, temp_dir)
                
                # Process COBOL files and store in database
                from cobol_parser import extract_cobol_files
                cobol_files = extract_cobol_files(temp_dir)
                
                programs_stored = 0
                for file_data in cobol_files:
                    # Prepare data for database
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
                    
                    # Read source code
                    if file_data.get('file_path') and os.path.exists(file_data['file_path']):
                        try:
                            with open(file_data['file_path'], 'r', encoding='utf-8', errors='ignore') as f:
                                program_data['source_code'] = f.read()
                        except Exception as e:
                            logging.error(f"Error reading source: {str(e)}")
                    
                    # Store in database
                    if store_cobol_program(program_data):
                        programs_stored += 1
                
                # Update session status
                update_session_status(session_id, 'completed', programs_stored)
                
                # Try to setup external services (optional)
                try:
                    setup_weaviate_schema()
                    run_ingestion_pipeline(temp_dir)
                    build_cobol_knowledge_graph()
                except Exception as e:
                    logging.warning(f"External services setup failed: {str(e)}")
                
                return jsonify({
                    'success': True, 
                    'message': f'Processing complete. {programs_stored} COBOL programs analyzed and stored.',
                    'programs_count': programs_stored
                })
                
            except Exception as e:
                logging.error(f"Processing error: {str(e)}")
                return jsonify({'error': f'Processing failed: {str(e)}'}), 500
            
            finally:
                # Clean up uploaded file
                if os.path.exists(filepath):
                    os.remove(filepath)
        
        else:
            return jsonify({'error': 'Invalid file type. Please upload ZIP, CBL, COB, or COBOL files.'}), 400
            
    except Exception as e:
        logging.error(f"Upload error: {str(e)}")
        return jsonify({'error': f'Upload failed: {str(e)}'}), 500

@app.route('/chat', methods=['POST'])
def chat():
    """Handle chat queries"""
    try:
        data = request.get_json()
        if not data or 'message' not in data:
            return jsonify({'error': 'No message provided'}), 400
        
        message = data['message'].strip()
        if not message:
            return jsonify({'error': 'Empty message'}), 400
        
        session_id = get_or_create_session()
        
        # Store user message
        store_chat_message(session_id, 'user', message)
        
        # Analyze query type and route to appropriate handler
        message_lower = message.lower()
        query_type = 'explanation'  # default
        
        if any(keyword in message_lower for keyword in ['depend', 'call', 'reference', 'use']):
            # Dependency analysis using database
            query_type = 'dependency'
            from database import get_programs_by_dependency, get_program_by_id
            
            # Extract program name from query
            import re
            patterns = [r'\b([A-Z][A-Z0-9\-]{2,})\b', r'program\s+([A-Z0-9\-]+)', r'called\s+([A-Z0-9\-]+)']
            program_name = None
            
            for pattern in patterns:
                matches = re.findall(pattern, message.upper())
                if matches:
                    program_name = matches[0]
                    break
            
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
                
        elif any(keyword in message_lower for keyword in ['similar', 'like', 'find', 'search']):
            # Similarity search using database
            query_type = 'similarity'
            from database import search_programs_by_text
            
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
                
        else:
            # Code explanation using database
            query_type = 'explanation'
            from database import get_program_by_id, search_programs_by_text
            
            # Extract program name
            import re
            patterns = [r'\b([A-Z][A-Z0-9\-]{2,})\b', r'program\s+([A-Z0-9\-]+)']
            program_name = None
            
            for pattern in patterns:
                matches = re.findall(pattern, message.upper())
                if matches:
                    program_name = matches[0]
                    break
            
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
                        response += f"Dependencies:\n"
                        for dep in deps:
                            response += f"- {dep}\n"
                else:
                    response = f"Program '{program_name}' not found in the database."
            else:
                # General search
                programs = search_programs_by_text(message, limit=3)
                if programs:
                    response = "Found programs related to your query:\n\n"
                    for i, program in enumerate(programs, 1):
                        response += f"{i}. {program.get('programId', program.get('fileName', 'Unknown'))}\n"
                        response += f"   Complexity: {program.get('complexity', 'Unknown')}\n"
                        response += f"   Lines: {program.get('lineCount', 0)}\n\n"
                else:
                    response = "No programs found matching your query. Please upload and process COBOL files first."
        
        # Store assistant response
        store_chat_message(session_id, 'assistant', response, query_type)
        
        return jsonify({
            'success': True,
            'response': response
        })
        
    except Exception as e:
        logging.error(f"Chat error: {str(e)}")
        return jsonify({'error': f'Query failed: {str(e)}'}), 500

@app.route('/health')
def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        try:
            db.session.execute(db.text('SELECT 1'))
            return jsonify({'status': 'healthy', 'database': 'connected'})
        except Exception as db_error:
            logging.error(f"Database connection failed: {str(db_error)}")
            return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 503
    except Exception as e:
        return jsonify({'status': 'unhealthy', 'error': str(e)}), 503

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
        logging.error(f"Analytics overview error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/relationships')
def get_relationship_analysis():
    """Get program relationship analysis"""
    try:
        relationships = analytics_service.analyze_program_relationships()
        return jsonify(relationships)
    except Exception as e:
        logging.error(f"Relationship analysis error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/refactoring')
def get_refactoring_opportunities():
    """Get refactoring opportunities"""
    try:
        opportunities = analytics_service.identify_refactoring_opportunities()
        return jsonify(opportunities)
    except Exception as e:
        logging.error(f"Refactoring analysis error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/llm/status')
def get_llm_status():
    """Get LLM provider status"""
    try:
        status = llm_service.get_provider_status()
        return jsonify(status)
    except Exception as e:
        logging.error(f"LLM status error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/llm/configure', methods=['POST'])
def configure_llm():
    """Configure custom LLM endpoint"""
    try:
        data = request.get_json()
        endpoint = data.get('endpoint')
        
        if endpoint:
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
        else:
            return jsonify({'error': 'Endpoint URL required'}), 400
            
    except Exception as e:
        logging.error(f"LLM configuration error: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.errorhandler(413)
def too_large(e):
    """Handle file too large error"""
    return jsonify({'error': 'File too large. Maximum size is 50MB.'}), 413

@app.errorhandler(500)
def internal_error(e):
    """Handle internal server errors"""
    logging.error(f"Internal error: {str(e)}")
    return jsonify({'error': 'Internal server error. Please try again.'}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
