"""
Graph Schema Versioning Service

Manages the knowledge graph schema with version control:
- Schema definition with node types, relationships, and properties
- Version tracking with semantic versioning
- Migration support for schema evolution
- Schema validation for nodes and relationships
- Schema export/diff between versions
"""

from typing import Dict, List, Optional, Any, Set
from datetime import datetime
from copy import deepcopy
import json
import structlog

logger = structlog.get_logger()


# ============================================================================
# DEFAULT SCHEMA (v1.0.0)
# ============================================================================

DEFAULT_SCHEMA = {
    'version': '1.0.0',
    'created_at': '2026-01-01T00:00:00Z',
    'node_types': {
        'PERSON': {
            'description': 'A person entity (colleague, author, contact)',
            'required_properties': ['name'],
            'optional_properties': ['email', 'organization', 'role', 'aliases'],
            'constraints': ['name must be non-empty string'],
        },
        'TOOL': {
            'description': 'A software tool or technology',
            'required_properties': ['name'],
            'optional_properties': ['category', 'url', 'version'],
            'constraints': [],
        },
        'TOPIC': {
            'description': 'A subject area, skill, or knowledge topic',
            'required_properties': ['name'],
            'optional_properties': ['category', 'difficulty', 'description'],
            'constraints': [],
        },
        'PROJECT': {
            'description': 'A project or repository',
            'required_properties': ['name'],
            'optional_properties': ['url', 'description', 'language', 'status'],
            'constraints': [],
        },
        'ORG': {
            'description': 'An organization or company',
            'required_properties': ['name'],
            'optional_properties': ['industry', 'url', 'size'],
            'constraints': [],
        },
        'PAPER': {
            'description': 'A research paper or document',
            'required_properties': ['title'],
            'optional_properties': ['doi', 'year', 'abstract', 'venue'],
            'constraints': [],
        },
        'EVENT': {
            'description': 'A calendar event, meeting, or conference',
            'required_properties': ['name'],
            'optional_properties': ['date', 'duration', 'location', 'type'],
            'constraints': [],
        },
        'PLACE': {
            'description': 'A geographic location',
            'required_properties': ['name'],
            'optional_properties': ['latitude', 'longitude', 'country', 'city'],
            'constraints': [],
        },
    },
    'relationship_types': {
        'CO_OCCURS_WITH': {
            'description': 'Entities appear in same activity',
            'source_types': ['*'],
            'target_types': ['*'],
            'properties': ['weight', 'count', 'last_seen'],
        },
        'WORKS_AT': {
            'description': 'Person works at organization',
            'source_types': ['PERSON'],
            'target_types': ['ORG'],
            'properties': ['since', 'role'],
        },
        'USES': {
            'description': 'Person or org uses a tool',
            'source_types': ['PERSON', 'ORG'],
            'target_types': ['TOOL'],
            'properties': ['proficiency', 'frequency'],
        },
        'AUTHORED': {
            'description': 'Person authored a paper or project',
            'source_types': ['PERSON'],
            'target_types': ['PAPER', 'PROJECT'],
            'properties': ['role', 'contribution'],
        },
        'ON_TOPIC': {
            'description': 'Entity is related to a topic',
            'source_types': ['*'],
            'target_types': ['TOPIC'],
            'properties': ['relevance', 'confidence'],
        },
        'COLLABORATES_WITH': {
            'description': 'Person collaborates with another person',
            'source_types': ['PERSON'],
            'target_types': ['PERSON'],
            'properties': ['strength', 'project_count'],
        },
        'DEPENDS_ON': {
            'description': 'Topic depends on another topic (prerequisite)',
            'source_types': ['TOPIC'],
            'target_types': ['TOPIC'],
            'properties': ['strength', 'type'],
        },
        'LOCATED_AT': {
            'description': 'Entity is located at a place',
            'source_types': ['PERSON', 'ORG', 'EVENT'],
            'target_types': ['PLACE'],
            'properties': ['since'],
        },
    },
    'indexes': [
        {'type': 'unique', 'node_type': 'PERSON', 'property': 'entity_id'},
        {'type': 'unique', 'node_type': 'TOOL', 'property': 'entity_id'},
        {'type': 'index', 'node_type': '*', 'property': 'user_id'},
        {'type': 'index', 'node_type': '*', 'property': 'canonical_name'},
    ],
}


class GraphSchemaService:
    """
    Service for managing and versioning the knowledge graph schema.

    Maintains a history of schema versions with diff and migration support.
    """

    def __init__(self, initial_schema: Optional[Dict[str, Any]] = None):
        self._current_schema = deepcopy(initial_schema or DEFAULT_SCHEMA)
        self._version_history: List[Dict[str, Any]] = [
            {
                'version': self._current_schema['version'],
                'schema': deepcopy(self._current_schema),
                'applied_at': self._current_schema.get('created_at', datetime.now(tz=None).isoformat()),
                'description': 'Initial schema',
            }
        ]
        self._migrations: List[Dict[str, Any]] = []

    @property
    def current_version(self) -> str:
        """Get current schema version."""
        return self._current_schema['version']

    @property
    def current_schema(self) -> Dict[str, Any]:
        """Get current schema (deep copy)."""
        return deepcopy(self._current_schema)

    def get_node_types(self) -> List[str]:
        """Get all defined node types."""
        return sorted(self._current_schema['node_types'].keys())

    def get_relationship_types(self) -> List[str]:
        """Get all defined relationship types."""
        return sorted(self._current_schema['relationship_types'].keys())

    def get_node_schema(self, node_type: str) -> Optional[Dict[str, Any]]:
        """Get schema for a specific node type."""
        schema = self._current_schema['node_types'].get(node_type)
        return deepcopy(schema) if schema else None

    def get_relationship_schema(self, rel_type: str) -> Optional[Dict[str, Any]]:
        """Get schema for a specific relationship type."""
        schema = self._current_schema['relationship_types'].get(rel_type)
        return deepcopy(schema) if schema else None

    def validate_node(
        self,
        node_type: str,
        properties: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Validate a node against the current schema.

        Args:
            node_type: Node type label
            properties: Node properties

        Returns:
            Validation result with errors and warnings
        """
        errors = []
        warnings = []

        node_schema = self._current_schema['node_types'].get(node_type)
        if node_schema is None:
            errors.append(f"Unknown node type: {node_type}")
            return {
                'valid': False,
                'errors': errors,
                'warnings': warnings,
                'node_type': node_type,
            }

        # Check required properties
        for prop in node_schema.get('required_properties', []):
            if prop not in properties or not properties[prop]:
                errors.append(f"Missing required property: {prop}")

        # Check for unknown properties
        known_props = set(
            node_schema.get('required_properties', []) +
            node_schema.get('optional_properties', []) +
            ['user_id', 'entity_id', 'created_at', 'updated_at', 'canonical_name']
        )
        for prop in properties:
            if prop not in known_props:
                warnings.append(f"Unknown property: {prop}")

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'node_type': node_type,
        }

    def validate_relationship(
        self,
        rel_type: str,
        source_type: str,
        target_type: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Validate a relationship against the current schema.

        Args:
            rel_type: Relationship type
            source_type: Source node type
            target_type: Target node type
            properties: Relationship properties

        Returns:
            Validation result
        """
        errors = []
        warnings = []
        properties = properties or {}

        rel_schema = self._current_schema['relationship_types'].get(rel_type)
        if rel_schema is None:
            errors.append(f"Unknown relationship type: {rel_type}")
            return {
                'valid': False,
                'errors': errors,
                'warnings': warnings,
                'relationship_type': rel_type,
            }

        # Check source type compatibility
        allowed_sources = rel_schema.get('source_types', ['*'])
        if '*' not in allowed_sources and source_type not in allowed_sources:
            errors.append(
                f"Invalid source type '{source_type}' for {rel_type}. "
                f"Allowed: {allowed_sources}"
            )

        # Check target type compatibility
        allowed_targets = rel_schema.get('target_types', ['*'])
        if '*' not in allowed_targets and target_type not in allowed_targets:
            errors.append(
                f"Invalid target type '{target_type}' for {rel_type}. "
                f"Allowed: {allowed_targets}"
            )

        return {
            'valid': len(errors) == 0,
            'errors': errors,
            'warnings': warnings,
            'relationship_type': rel_type,
        }

    def add_node_type(
        self,
        type_name: str,
        description: str,
        required_properties: Optional[List[str]] = None,
        optional_properties: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Add a new node type to the schema.

        Returns:
            Migration record
        """
        if type_name in self._current_schema['node_types']:
            return {'success': False, 'error': f"Node type {type_name} already exists"}

        self._current_schema['node_types'][type_name] = {
            'description': description,
            'required_properties': required_properties or [],
            'optional_properties': optional_properties or [],
            'constraints': [],
        }

        migration = {
            'type': 'add_node_type',
            'node_type': type_name,
            'applied_at': datetime.now(tz=None).isoformat(),
        }
        self._migrations.append(migration)

        return {'success': True, 'migration': migration}

    def add_relationship_type(
        self,
        rel_type: str,
        description: str,
        source_types: Optional[List[str]] = None,
        target_types: Optional[List[str]] = None,
        properties: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Add a new relationship type to the schema.

        Returns:
            Migration record
        """
        if rel_type in self._current_schema['relationship_types']:
            return {'success': False, 'error': f"Relationship type {rel_type} already exists"}

        self._current_schema['relationship_types'][rel_type] = {
            'description': description,
            'source_types': source_types or ['*'],
            'target_types': target_types or ['*'],
            'properties': properties or [],
        }

        migration = {
            'type': 'add_relationship_type',
            'relationship_type': rel_type,
            'applied_at': datetime.now(tz=None).isoformat(),
        }
        self._migrations.append(migration)

        return {'success': True, 'migration': migration}

    def bump_version(
        self,
        bump_type: str = 'minor',
        description: str = '',
    ) -> Dict[str, Any]:
        """
        Create a new schema version.

        Args:
            bump_type: 'major', 'minor', or 'patch'
            description: Version description

        Returns:
            New version info
        """
        parts = self._current_schema['version'].split('.')
        major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

        if bump_type == 'major':
            major += 1
            minor = 0
            patch = 0
        elif bump_type == 'minor':
            minor += 1
            patch = 0
        else:  # patch
            patch += 1

        new_version = f"{major}.{minor}.{patch}"
        self._current_schema['version'] = new_version

        version_entry = {
            'version': new_version,
            'schema': deepcopy(self._current_schema),
            'applied_at': datetime.now(tz=None).isoformat(),
            'description': description,
            'migrations': list(self._migrations),
        }
        self._version_history.append(version_entry)
        self._migrations = []

        return {
            'previous_version': f"{parts[0]}.{parts[1]}.{parts[2]}",
            'new_version': new_version,
            'description': description,
            'migration_count': len(version_entry.get('migrations', [])),
        }

    def diff_versions(
        self,
        version_a: Optional[str] = None,
        version_b: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compare two schema versions.

        Args:
            version_a: First version (default: previous)
            version_b: Second version (default: current)

        Returns:
            Diff between versions
        """
        if len(self._version_history) < 2:
            return {'changes': [], 'message': 'Only one version exists'}

        schema_a = None
        schema_b = None

        for entry in self._version_history:
            if version_a and entry['version'] == version_a:
                schema_a = entry['schema']
            if version_b and entry['version'] == version_b:
                schema_b = entry['schema']

        if not schema_a:
            schema_a = self._version_history[-2]['schema']
        if not schema_b:
            schema_b = self._version_history[-1]['schema']

        changes = []

        # Compare node types
        old_nodes = set(schema_a.get('node_types', {}).keys())
        new_nodes = set(schema_b.get('node_types', {}).keys())

        for added in (new_nodes - old_nodes):
            changes.append({'type': 'node_type_added', 'name': added})
        for removed in (old_nodes - new_nodes):
            changes.append({'type': 'node_type_removed', 'name': removed})

        # Compare relationship types
        old_rels = set(schema_a.get('relationship_types', {}).keys())
        new_rels = set(schema_b.get('relationship_types', {}).keys())

        for added in (new_rels - old_rels):
            changes.append({'type': 'relationship_type_added', 'name': added})
        for removed in (old_rels - new_rels):
            changes.append({'type': 'relationship_type_removed', 'name': removed})

        # Compare property changes for existing types
        for node_type in (old_nodes & new_nodes):
            old_props = set(
                schema_a['node_types'][node_type].get('required_properties', []) +
                schema_a['node_types'][node_type].get('optional_properties', [])
            )
            new_props = set(
                schema_b['node_types'][node_type].get('required_properties', []) +
                schema_b['node_types'][node_type].get('optional_properties', [])
            )

            for added in (new_props - old_props):
                changes.append({
                    'type': 'property_added',
                    'node_type': node_type,
                    'property': added,
                })
            for removed in (old_props - new_props):
                changes.append({
                    'type': 'property_removed',
                    'node_type': node_type,
                    'property': removed,
                })

        return {
            'version_a': schema_a.get('version', '?'),
            'version_b': schema_b.get('version', '?'),
            'total_changes': len(changes),
            'changes': changes,
        }

    def export_schema(self, as_json: bool = False) -> Any:
        """
        Export the current schema.

        Args:
            as_json: If True, return JSON string; else return dict

        Returns:
            Schema dict or JSON string
        """
        schema = deepcopy(self._current_schema)
        schema['exported_at'] = datetime.now(tz=None).isoformat()

        if as_json:
            return json.dumps(schema, indent=2, default=str)
        return schema

    def get_version_history(self) -> List[Dict[str, Any]]:
        """Get all schema versions (without full schema data)."""
        return [
            {
                'version': entry['version'],
                'applied_at': entry['applied_at'],
                'description': entry.get('description', ''),
                'migration_count': len(entry.get('migrations', [])),
            }
            for entry in self._version_history
        ]

    def get_statistics(self) -> Dict[str, Any]:
        """Get schema statistics."""
        node_types = self._current_schema.get('node_types', {})
        rel_types = self._current_schema.get('relationship_types', {})

        total_properties = sum(
            len(nt.get('required_properties', [])) + len(nt.get('optional_properties', []))
            for nt in node_types.values()
        )

        return {
            'version': self._current_schema['version'],
            'node_type_count': len(node_types),
            'relationship_type_count': len(rel_types),
            'total_properties': total_properties,
            'total_versions': len(self._version_history),
            'index_count': len(self._current_schema.get('indexes', [])),
        }


# Global instance
graph_schema_service = GraphSchemaService()
