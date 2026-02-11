package schema

import (
	"strings"
)

// QualifiedName represents a fully qualified database object name
type QualifiedName struct {
	Database string
	Schema   string
	Table    string
}

// ParseQualifiedName parses a qualified name into its components.
// Handles: 'Table', 'Schema.Table', 'Database.Schema.Table'
// And their bracketed equivalents: '[Database].[Schema].[Table]'
func ParseQualifiedName(fullName string) QualifiedName {
	// Normalize by removing brackets first for parsing
	normalized := NormalizeBrackets(fullName)
	parts := strings.Split(normalized, ".")

	result := QualifiedName{}

	switch len(parts) {
	case 1:
		// Just table name
		result.Table = parts[0]
	case 2:
		// Schema.Table
		result.Schema = parts[0]
		result.Table = parts[1]
	case 3:
		// Database.Schema.Table
		result.Database = parts[0]
		result.Schema = parts[1]
		result.Table = parts[2]
	default:
		// Handle more than 3 parts (take last 3)
		if len(parts) > 3 {
			result.Database = parts[len(parts)-3]
			result.Schema = parts[len(parts)-2]
			result.Table = parts[len(parts)-1]
		}
	}

	return result
}

// NormalizeBrackets removes brackets from an identifier.
// '[Database].[Schema].[Table]' -> 'Database.Schema.Table'
func NormalizeBrackets(identifier string) string {
	return strings.ReplaceAll(strings.ReplaceAll(identifier, "[", ""), "]", "")
}

// BracketIdentifier adds brackets to an identifier if not present.
// 'MyTable' -> '[MyTable]'
// '[MyTable]' -> '[MyTable]' (no change)
func BracketIdentifier(identifier string) string {
	identifier = strings.TrimSpace(identifier)
	if identifier == "" {
		return identifier
	}

	// If already bracketed, return as-is
	if strings.HasPrefix(identifier, "[") && strings.HasSuffix(identifier, "]") {
		return identifier
	}

	return "[" + identifier + "]"
}

// BuildQualifiedName builds a fully qualified name with proper bracketing.
func BuildQualifiedName(table, schema, database string, useBrackets bool) string {
	var parts []string

	if database != "" {
		if useBrackets {
			parts = append(parts, BracketIdentifier(database))
		} else {
			parts = append(parts, database)
		}
	}
	if schema != "" {
		if useBrackets {
			parts = append(parts, BracketIdentifier(schema))
		} else {
			parts = append(parts, schema)
		}
	}
	if table != "" {
		if useBrackets {
			parts = append(parts, BracketIdentifier(table))
		} else {
			parts = append(parts, table)
		}
	}

	return strings.Join(parts, ".")
}

// StandardizeTableName standardizes a table name to a consistent format.
// If useBrackets is true, returns '[Database].[Schema].[Table]' format.
// Otherwise returns 'Database.Schema.Table' format.
func StandardizeTableName(tableName, defaultDB, defaultSchema string, useBrackets bool) string {
	if tableName == "" {
		return tableName
	}

	parsed := ParseQualifiedName(tableName)

	// Fill in missing components
	db := parsed.Database
	if db == "" {
		db = defaultDB
	}

	schema := parsed.Schema
	if schema == "" {
		schema = defaultSchema
	}

	table := parsed.Table

	return BuildQualifiedName(table, schema, db, useBrackets)
}

// ExtractDatabaseName extracts database name from filename.
// 'dv_schema.json' -> 'DV'
// 'my-database-schema.json' -> 'MY-DATABASE'
func ExtractDatabaseName(filepath string) string {
	// Get filename without extension
	filename := filepath
	if idx := strings.LastIndex(filename, "/"); idx >= 0 {
		filename = filename[idx+1:]
	}
	if idx := strings.LastIndex(filename, "\\"); idx >= 0 {
		filename = filename[idx+1:]
	}
	if idx := strings.LastIndex(filename, "."); idx >= 0 {
		filename = filename[:idx]
	}

	// Remove common suffixes
	filename = strings.TrimSuffix(filename, "_schema")
	filename = strings.TrimSuffix(filename, "-schema")

	return strings.ToUpper(filename)
}
