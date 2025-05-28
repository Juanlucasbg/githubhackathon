"""
Database models for COBOL Companion AI system.
Stores processed COBOL programs, analysis results, and user interactions.
"""

from datetime import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy import JSON

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)

class CobolProgram(db.Model):
    """Model for storing processed COBOL programs"""
    __tablename__ = 'cobol_programs'
    
    id = db.Column(db.Integer, primary_key=True)
    program_id = db.Column(db.String(100), unique=True, nullable=False, index=True)
    file_name = db.Column(db.String(255), nullable=False)
    file_path = db.Column(db.Text)
    source_code = db.Column(db.Text)
    ast_structure = db.Column(JSON)
    procedures = db.Column(JSON)
    data_divisions = db.Column(JSON)
    dependencies = db.Column(JSON)
    copybooks = db.Column(JSON)
    business_rules = db.Column(db.Text)
    complexity = db.Column(db.String(20))
    line_count = db.Column(db.Integer, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def to_dict(self):
        """Convert model instance to dictionary"""
        return {
            'id': self.id,
            'programId': self.program_id,
            'fileName': self.file_name,
            'filePath': self.file_path,
            'sourceCode': self.source_code,
            'astStructure': self.ast_structure,
            'procedures': self.procedures,
            'dataDivisions': self.data_divisions,
            'dependencies': self.dependencies,
            'copybooks': self.copybooks,
            'businessRules': self.business_rules,
            'complexity': self.complexity,
            'lineCount': self.line_count,
            'createdAt': self.created_at.isoformat() if self.created_at else None,
            'updatedAt': self.updated_at.isoformat() if self.updated_at else None
        }

class AnalysisSession(db.Model):
    """Model for tracking analysis sessions"""
    __tablename__ = 'analysis_sessions'
    
    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.String(100), unique=True, nullable=False, index=True)
    uploaded_files = db.Column(JSON)
    programs_count = db.Column(db.Integer, default=0)
    processing_status = db.Column(db.String(50), default='pending')
    error_message = db.Column(db.Text)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    completed_at = db.Column(db.DateTime)
    
    # Relationships
    chat_messages = db.relationship('ChatMessage', backref='session', lazy='dynamic')

class ChatMessage(db.Model):
    """Model for storing chat interactions"""
    __tablename__ = 'chat_messages'
    
    id = db.Column(db.Integer, primary_key=True)
    session_id = db.Column(db.String(100), db.ForeignKey('analysis_sessions.session_id'), nullable=False)
    message_type = db.Column(db.String(20), nullable=False)  # 'user' or 'assistant'
    content = db.Column(db.Text, nullable=False)
    query_type = db.Column(db.String(50))  # 'dependency', 'similarity', 'explanation'
    response_time_ms = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class ProgramDependency(db.Model):
    """Model for storing program dependency relationships"""
    __tablename__ = 'program_dependencies'
    
    id = db.Column(db.Integer, primary_key=True)
    source_program_id = db.Column(db.String(100), nullable=False, index=True)
    target_program_id = db.Column(db.String(100), nullable=False, index=True)
    dependency_type = db.Column(db.String(50), nullable=False)  # 'CALL', 'COPY', etc.
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('source_program_id', 'target_program_id', 'dependency_type'),
    )

class KnowledgeGraphNode(db.Model):
    """Model for storing knowledge graph nodes"""
    __tablename__ = 'knowledge_graph_nodes'
    
    id = db.Column(db.Integer, primary_key=True)
    node_id = db.Column(db.String(100), unique=True, nullable=False, index=True)
    node_type = db.Column(db.String(50), nullable=False)  # 'PROGRAM', 'COPYBOOK', etc.
    content = db.Column(db.Text)
    node_metadata = db.Column(JSON)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class KnowledgeGraphRelationship(db.Model):
    """Model for storing knowledge graph relationships"""
    __tablename__ = 'knowledge_graph_relationships'
    
    id = db.Column(db.Integer, primary_key=True)
    source_node_id = db.Column(db.String(100), nullable=False, index=True)
    target_node_id = db.Column(db.String(100), nullable=False, index=True)
    relationship_type = db.Column(db.String(50), nullable=False)
    strength = db.Column(db.Float, default=1.0)
    rel_metadata = db.Column(JSON)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        db.UniqueConstraint('source_node_id', 'target_node_id', 'relationship_type'),
    )