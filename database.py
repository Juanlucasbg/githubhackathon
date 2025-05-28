"""
Database configuration and operations for COBOL Companion AI.
"""

import os
import logging
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from models import db, CobolProgram, AnalysisSession, ChatMessage, ProgramDependency

def init_database(app):
    """Initialize database with app context"""
    with app.app_context():
        try:
            # Create all tables
            db.create_all()
            logging.info("Database tables created successfully")
            return True
        except Exception as e:
            logging.error(f"Error creating database tables: {str(e)}")
            return False

def store_cobol_program(program_data):
    """Store COBOL program in PostgreSQL database"""
    try:
        # Check if program already exists
        existing = CobolProgram.query.filter_by(
            program_id=program_data.get('program_id', 'unknown')
        ).first()
        
        if existing:
            # Update existing program
            for key, value in program_data.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
            existing.updated_at = datetime.utcnow()
        else:
            # Create new program
            program = CobolProgram()
            for key, value in program_data.items():
                if hasattr(program, key):
                    setattr(program, key, value)
            db.session.add(program)
        
        db.session.commit()
        logging.info(f"Stored COBOL program: {program_data.get('program_id', 'unknown')}")
        return True
        
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error storing COBOL program: {str(e)}")
        return False

def get_programs_by_dependency(dependency_name):
    """Get programs that depend on a specific module"""
    try:
        programs = CobolProgram.query.filter(
            CobolProgram.dependencies.contains([dependency_name])
        ).all()
        return [program.to_dict() for program in programs]
    except Exception as e:
        logging.error(f"Error querying programs by dependency: {str(e)}")
        return []

def search_programs_by_text(query_text, limit=10):
    """Search programs by text content"""
    try:
        # Simple text search in source code and program_id
        programs = CobolProgram.query.filter(
            db.or_(
                CobolProgram.source_code.contains(query_text.upper()),
                CobolProgram.program_id.contains(query_text.upper())
            )
        ).limit(limit).all()
        
        return [program.to_dict() for program in programs]
    except Exception as e:
        logging.error(f"Error searching programs: {str(e)}")
        return []

def get_program_by_id(program_id):
    """Get a specific program by its ID"""
    try:
        program = CobolProgram.query.filter_by(program_id=program_id).first()
        return program.to_dict() if program else None
    except Exception as e:
        logging.error(f"Error getting program by ID: {str(e)}")
        return None

def store_chat_message(session_id, message_type, content, query_type=None):
    """Store chat message in database"""
    try:
        message = ChatMessage()
        message.session_id = session_id
        message.message_type = message_type
        message.content = content
        message.query_type = query_type
        
        db.session.add(message)
        db.session.commit()
        return True
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error storing chat message: {str(e)}")
        return False

def get_session_status(session_id):
    """Get analysis session status"""
    try:
        session_obj = AnalysisSession.query.filter_by(session_id=session_id).first()
        if session_obj:
            return {
                'session_id': session_obj.session_id,
                'programs_count': session_obj.programs_count,
                'processing_status': session_obj.processing_status,
                'created_at': session_obj.created_at.isoformat() if session_obj.created_at else None
            }
        return None
    except Exception as e:
        logging.error(f"Error getting session status: {str(e)}")
        return None

def update_session_status(session_id, status, programs_count=None, error_message=None):
    """Update analysis session status"""
    try:
        session_obj = AnalysisSession.query.filter_by(session_id=session_id).first()
        if session_obj:
            session_obj.processing_status = status
            if programs_count is not None:
                session_obj.programs_count = programs_count
            if error_message:
                session_obj.error_message = error_message
            if status == 'completed':
                session_obj.completed_at = datetime.utcnow()
            
            db.session.commit()
            return True
        return False
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error updating session status: {str(e)}")
        return False

def get_all_programs():
    """Get all COBOL programs from database"""
    try:
        programs = CobolProgram.query.all()
        return [program.to_dict() for program in programs]
    except Exception as e:
        logging.error(f"Error getting all programs: {str(e)}")
        return []

def delete_all_programs():
    """Delete all COBOL programs from database"""
    try:
        CobolProgram.query.delete()
        db.session.commit()
        logging.info("All COBOL programs deleted from database")
        return True
    except Exception as e:
        db.session.rollback()
        logging.error(f"Error deleting programs: {str(e)}")
        return False