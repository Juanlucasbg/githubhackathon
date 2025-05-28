"""
Advanced Analytics Service for COBOL CodeSense
Provides comprehensive analytics, visualization, and reporting capabilities
"""

import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json

try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.offline import plot
    import networkx as nx
    VISUALIZATION_AVAILABLE = True
except ImportError:
    VISUALIZATION_AVAILABLE = False

from database import get_all_programs, search_programs_by_text

class AnalyticsService:
    """Advanced analytics for COBOL codebase analysis"""
    
    def __init__(self):
        self.visualization_enabled = VISUALIZATION_AVAILABLE
        if not self.visualization_enabled:
            logging.warning("Visualization libraries not available")
    
    def generate_codebase_overview(self) -> Dict[str, Any]:
        """Generate comprehensive codebase overview"""
        try:
            programs = get_all_programs()
            
            if not programs:
                return {
                    "total_programs": 0,
                    "message": "No COBOL programs found. Please upload and process your files first."
                }
            
            # Calculate metrics
            total_programs = len(programs)
            total_lines = sum(p.get('lineCount', 0) for p in programs)
            
            # Complexity distribution
            complexity_dist = {}
            for program in programs:
                complexity = program.get('complexity', 'Unknown')
                complexity_dist[complexity] = complexity_dist.get(complexity, 0) + 1
            
            # Dependency analysis
            all_dependencies = []
            for program in programs:
                deps = program.get('dependencies', [])
                all_dependencies.extend(deps)
            
            unique_dependencies = len(set(all_dependencies))
            most_common_deps = self._get_most_common_items(all_dependencies, 5)
            
            # Program size distribution
            size_categories = self._categorize_by_size(programs)
            
            overview = {
                "total_programs": total_programs,
                "total_lines_of_code": total_lines,
                "average_lines_per_program": total_lines // total_programs if total_programs > 0 else 0,
                "complexity_distribution": complexity_dist,
                "total_unique_dependencies": unique_dependencies,
                "most_common_dependencies": most_common_deps,
                "size_distribution": size_categories,
                "generated_at": datetime.now().isoformat()
            }
            
            return overview
            
        except Exception as e:
            logging.error(f"Error generating codebase overview: {str(e)}")
            return {"error": str(e)}
    
    def analyze_program_relationships(self) -> Dict[str, Any]:
        """Analyze relationships between COBOL programs"""
        try:
            programs = get_all_programs()
            
            if not programs:
                return {"message": "No programs available for relationship analysis"}
            
            # Build relationship graph
            relationships = []
            dependency_graph = {}
            
            for program in programs:
                program_id = program.get('programId', 'Unknown')
                dependencies = program.get('dependencies', [])
                
                dependency_graph[program_id] = dependencies
                
                for dep in dependencies:
                    relationships.append({
                        "source": program_id,
                        "target": dep,
                        "type": "depends_on"
                    })
            
            # Calculate metrics
            total_relationships = len(relationships)
            programs_with_deps = len([p for p in programs if p.get('dependencies')])
            isolated_programs = len(programs) - programs_with_deps
            
            # Find highly connected programs
            dep_counts = {}
            for rel in relationships:
                target = rel['target']
                dep_counts[target] = dep_counts.get(target, 0) + 1
            
            most_depended_on = sorted(dep_counts.items(), key=lambda x: x[1], reverse=True)[:5]
            
            return {
                "total_relationships": total_relationships,
                "programs_with_dependencies": programs_with_deps,
                "isolated_programs": isolated_programs,
                "most_depended_on_modules": most_depended_on,
                "dependency_graph": dependency_graph,
                "relationship_details": relationships
            }
            
        except Exception as e:
            logging.error(f"Error analyzing relationships: {str(e)}")
            return {"error": str(e)}
    
    def generate_dependency_visualization(self) -> Optional[str]:
        """Generate HTML visualization of program dependencies"""
        if not self.visualization_enabled:
            return None
        
        try:
            relationship_data = self.analyze_program_relationships()
            relationships = relationship_data.get('relationship_details', [])
            
            if not relationships:
                return None
            
            # Create network graph
            G = nx.DiGraph()
            
            for rel in relationships:
                G.add_edge(rel['source'], rel['target'])
            
            # Calculate layout
            pos = nx.spring_layout(G)
            
            # Extract coordinates
            edge_x = []
            edge_y = []
            
            for edge in G.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])
            
            # Create edge trace
            edge_trace = go.Scatter(
                x=edge_x, y=edge_y,
                line=dict(width=0.5, color='#888'),
                hoverinfo='none',
                mode='lines'
            )
            
            # Create node trace
            node_x = []
            node_y = []
            node_text = []
            
            for node in G.nodes():
                x, y = pos[node]
                node_x.append(x)
                node_y.append(y)
                node_text.append(node)
            
            node_trace = go.Scatter(
                x=node_x, y=node_y,
                mode='markers+text',
                hoverinfo='text',
                text=node_text,
                textposition="middle center",
                marker=dict(
                    size=20,
                    color='lightblue',
                    line=dict(width=2, color='darkblue')
                )
            )
            
            # Create figure
            fig = go.Figure(
                data=[edge_trace, node_trace],
                layout=go.Layout(
                    title='COBOL Program Dependency Network',
                    showlegend=False,
                    hovermode='closest',
                    margin=dict(b=20,l=5,r=5,t=40),
                    annotations=[
                        dict(
                            text="Program dependencies visualization",
                            showarrow=False,
                            xref="paper", yref="paper",
                            x=0.005, y=-0.002,
                            xanchor='left', yanchor='bottom',
                            font=dict(color="#000000", size=12)
                        )
                    ],
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)
                )
            )
            
            # Generate HTML
            html_content = plot(fig, output_type='div', include_plotlyjs=True)
            return html_content
            
        except Exception as e:
            logging.error(f"Error generating visualization: {str(e)}")
            return None
    
    def generate_complexity_chart(self) -> Optional[str]:
        """Generate complexity distribution chart"""
        if not self.visualization_enabled:
            return None
        
        try:
            overview = self.generate_codebase_overview()
            complexity_dist = overview.get('complexity_distribution', {})
            
            if not complexity_dist:
                return None
            
            fig = px.pie(
                values=list(complexity_dist.values()),
                names=list(complexity_dist.keys()),
                title='COBOL Program Complexity Distribution'
            )
            
            fig.update_traces(
                textposition='inside',
                textinfo='percent+label'
            )
            
            html_content = plot(fig, output_type='div', include_plotlyjs=True)
            return html_content
            
        except Exception as e:
            logging.error(f"Error generating complexity chart: {str(e)}")
            return None
    
    def identify_refactoring_opportunities(self) -> Dict[str, Any]:
        """Identify potential refactoring opportunities"""
        try:
            programs = get_all_programs()
            
            if not programs:
                return {"message": "No programs available for analysis"}
            
            opportunities = {
                "high_complexity_programs": [],
                "large_programs": [],
                "highly_dependent_programs": [],
                "isolated_programs": [],
                "duplicate_dependencies": []
            }
            
            # Find high complexity programs
            for program in programs:
                complexity = program.get('complexity', 'Unknown')
                line_count = program.get('lineCount', 0)
                dependencies = program.get('dependencies', [])
                
                if complexity == 'High':
                    opportunities["high_complexity_programs"].append({
                        "program_id": program.get('programId'),
                        "line_count": line_count,
                        "complexity": complexity
                    })
                
                if line_count > 1000:
                    opportunities["large_programs"].append({
                        "program_id": program.get('programId'),
                        "line_count": line_count
                    })
                
                if len(dependencies) > 10:
                    opportunities["highly_dependent_programs"].append({
                        "program_id": program.get('programId'),
                        "dependency_count": len(dependencies)
                    })
                
                if not dependencies:
                    opportunities["isolated_programs"].append({
                        "program_id": program.get('programId'),
                        "line_count": line_count
                    })
            
            # Find duplicate dependency patterns
            dependency_patterns = {}
            for program in programs:
                deps = tuple(sorted(program.get('dependencies', [])))
                if deps and len(deps) > 1:
                    if deps not in dependency_patterns:
                        dependency_patterns[deps] = []
                    dependency_patterns[deps].append(program.get('programId'))
            
            for pattern, programs_list in dependency_patterns.items():
                if len(programs_list) > 1:
                    opportunities["duplicate_dependencies"].append({
                        "pattern": list(pattern),
                        "programs": programs_list
                    })
            
            return opportunities
            
        except Exception as e:
            logging.error(f"Error identifying refactoring opportunities: {str(e)}")
            return {"error": str(e)}
    
    def _get_most_common_items(self, items: List[str], limit: int) -> List[Dict[str, Any]]:
        """Get most common items from a list"""
        from collections import Counter
        counter = Counter(items)
        return [{"name": item, "count": count} for item, count in counter.most_common(limit)]
    
    def _categorize_by_size(self, programs: List[Dict[str, Any]]) -> Dict[str, int]:
        """Categorize programs by size"""
        categories = {
            "Small (< 100 lines)": 0,
            "Medium (100-500 lines)": 0,
            "Large (500-1000 lines)": 0,
            "Very Large (> 1000 lines)": 0
        }
        
        for program in programs:
            line_count = program.get('lineCount', 0)
            
            if line_count < 100:
                categories["Small (< 100 lines)"] += 1
            elif line_count < 500:
                categories["Medium (100-500 lines)"] += 1
            elif line_count < 1000:
                categories["Large (500-1000 lines)"] += 1
            else:
                categories["Very Large (> 1000 lines)"] += 1
        
        return categories
    
    def generate_analytics_report(self) -> Dict[str, Any]:
        """Generate comprehensive analytics report"""
        try:
            report = {
                "generated_at": datetime.now().isoformat(),
                "codebase_overview": self.generate_codebase_overview(),
                "relationship_analysis": self.analyze_program_relationships(),
                "refactoring_opportunities": self.identify_refactoring_opportunities(),
                "visualizations_available": self.visualization_enabled
            }
            
            if self.visualization_enabled:
                report["dependency_visualization"] = self.generate_dependency_visualization()
                report["complexity_chart"] = self.generate_complexity_chart()
            
            return report
            
        except Exception as e:
            logging.error(f"Error generating analytics report: {str(e)}")
            return {"error": str(e)}

# Global analytics service instance
analytics_service = AnalyticsService()