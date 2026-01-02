package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// MergeConfig contains configuration for merging multiple schemas
type MergeConfig struct {
	// Name for the merged schema
	Name string
	// Description for the merged schema
	Description string
	// Default schema name (e.g., "dbo" for SQL Server)
	DefaultSchema string
	// Use bracket notation for identifiers ([Database].[Schema].[Table])
	UseBrackets bool
	// Extract virtual relations from view JOINs
	ExtractViewRelations bool
	// Database name mapping (filename -> database name)
	DatabaseMapping map[string]string
}

// MergeStats contains statistics about the merge operation
type MergeStats struct {
	TotalTables        int
	TotalViews         int
	TotalRelations     int
	TotalFunctions     int
	Databases          []string
	CrossDBRelations   int
	ExtractedRelations int
	DeduplicatedCount  int
}

// MergeSchemas merges multiple tbls schemas with standardized naming and virtual relation extraction.
func MergeSchemas(jsonFiles []string, config *MergeConfig) (*Schema, *MergeStats, error) {
	if config == nil {
		config = &MergeConfig{
			Name:                 "Combined Schema",
			Description:          fmt.Sprintf("Combined schema from %d databases", len(jsonFiles)),
			DefaultSchema:        "dbo",
			UseBrackets:          true,
			ExtractViewRelations: true,
			DatabaseMapping:      make(map[string]string),
		}
	}

	merged := &Schema{
		Name:      config.Name,
		Desc:      config.Description,
		Tables:    []*Table{},
		Relations: []*Relation{},
		Functions: []*Function{},
	}

	stats := &MergeStats{
		Databases: []string{},
	}

	for _, jsonFile := range jsonFiles {
		// Load schema from JSON file
		schema, err := LoadSchemaFromJSON(jsonFile)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to load schema from %s: %w", jsonFile, err)
		}

		// Extract database prefix from filename or mapping
		var dbPrefix string
		if mappedName, ok := config.DatabaseMapping[jsonFile]; ok {
			dbPrefix = mappedName
		} else {
			dbPrefix = ExtractDatabaseName(jsonFile)
		}

		stats.Databases = append(stats.Databases, dbPrefix)

		// Get schema name from driver metadata or use default
		schemaName := config.DefaultSchema
		if schema.Driver != nil && schema.Driver.Meta != nil && schema.Driver.Meta.CurrentSchema != "" {
			schemaName = strings.Trim(schema.Driver.Meta.CurrentSchema, "\"")
		}

		// Process tables
		updatedTables := updateTableNames(schema.Tables, dbPrefix, schemaName, config.UseBrackets)
		merged.Tables = append(merged.Tables, updatedTables...)
		stats.TotalTables += len(updatedTables)

		// Count views
		for _, t := range updatedTables {
			if t.Type == "VIEW" || t.Type == "MATERIALIZED VIEW" {
				stats.TotalViews++
			}
		}

		// Process existing relations from foreign keys
		updatedRelations := updateRelations(schema.Relations, dbPrefix, schemaName, config.UseBrackets)

		// Count cross-database relations
		for _, rel := range updatedRelations {
			relDB := ParseQualifiedName(rel.Table.Name).Database
			parentDB := ParseQualifiedName(rel.ParentTable.Name).Database
			if relDB != "" && parentDB != "" && relDB != parentDB {
				stats.CrossDBRelations++
			}
		}

		merged.Relations = append(merged.Relations, updatedRelations...)
		stats.TotalRelations += len(updatedRelations)

		// Extract relations from view definitions if enabled
		if config.ExtractViewRelations {
			virtualRels := ExtractRelationsFromDefinitions(updatedTables, dbPrefix, schemaName, config.UseBrackets)
			merged.Relations = append(merged.Relations, virtualRels...)
			stats.ExtractedRelations += len(virtualRels)
		}

		// Process functions
		updatedFunctions := updateFunctions(schema.Functions, dbPrefix, schemaName, config.UseBrackets)
		merged.Functions = append(merged.Functions, updatedFunctions...)
		stats.TotalFunctions += len(updatedFunctions)

		// Copy driver info from first schema
		if merged.Driver == nil && schema.Driver != nil {
			merged.Driver = schema.Driver
		}
	}

	// Deduplicate relations (prefer FK constraints over extracted relations)
	originalRelationCount := len(merged.Relations)
	merged.Relations = DeduplicateRelations(merged.Relations)
	stats.DeduplicatedCount = originalRelationCount - len(merged.Relations)

	// Repair schema to connect relations to tables
	if err := merged.Repair(); err != nil {
		return nil, nil, fmt.Errorf("failed to repair merged schema: %w", err)
	}

	return merged, stats, nil
}

// LoadSchemaFromJSON loads a schema from a JSON file
func LoadSchemaFromJSON(filepath string) (*Schema, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	var schema Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal JSON: %w", err)
	}

	return &schema, nil
}

// SaveSchemaToJSON saves a schema to a JSON file
func SaveSchemaToJSON(schema *Schema, filepath string) error {
	data, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	if err := os.WriteFile(filepath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	return nil
}

// updateTableNames updates table names with standardized format
func updateTableNames(tables []*Table, dbPrefix, schemaName string, useBrackets bool) []*Table {
	updated := make([]*Table, len(tables))

	for i, table := range tables {
		// Create a copy of the table
		t := *table

		// Update table name
		originalName := t.Name
		t.Name = StandardizeTableName(originalName, dbPrefix, schemaName, useBrackets)

		// Update referenced tables if present
		if len(t.ReferencedTables) > 0 {
			refTables := make([]*Table, len(t.ReferencedTables))
			for j, ref := range t.ReferencedTables {
				stdName := StandardizeTableName(ref.Name, dbPrefix, schemaName, useBrackets)
				refTables[j] = &Table{Name: stdName}
			}
			t.ReferencedTables = refTables
		}

		// Update constraints
		if len(t.Constraints) > 0 {
			constraints := make([]*Constraint, len(t.Constraints))
			for j, constraint := range t.Constraints {
				c := *constraint
				if c.Table != nil {
					stdName := StandardizeTableName(*c.Table, dbPrefix, schemaName, useBrackets)
					c.Table = &stdName
				}
				if c.ReferencedTable != nil && *c.ReferencedTable != "" {
					// For referenced tables, we might not know the database
					// Try to parse it, but keep the original database if specified
					refParsed := ParseQualifiedName(*c.ReferencedTable)
					refDB := refParsed.Database
					if refDB == "" {
						refDB = dbPrefix
					}
					stdName := StandardizeTableName(*c.ReferencedTable, refDB, schemaName, useBrackets)
					c.ReferencedTable = &stdName
				}
				constraints[j] = &c
			}
			t.Constraints = constraints
		}

		// Update indexes
		if len(t.Indexes) > 0 {
			indexes := make([]*Index, len(t.Indexes))
			for j, index := range t.Indexes {
				idx := *index
				if idx.Table != nil {
					stdName := StandardizeTableName(*idx.Table, dbPrefix, schemaName, useBrackets)
					idx.Table = &stdName
				}
				indexes[j] = &idx
			}
			t.Indexes = indexes
		}

		updated[i] = &t
	}

	return updated
}

// updateRelations updates relation references with standardized format
func updateRelations(relations []*Relation, dbPrefix, schemaName string, useBrackets bool) []*Relation {
	updated := make([]*Relation, len(relations))

	for i, relation := range relations {
		r := *relation

		// Parse to detect if cross-database reference
		tableParsed := ParseQualifiedName(r.Table.Name)
		parentParsed := ParseQualifiedName(r.ParentTable.Name)

		// Use specified database or default
		tableDB := tableParsed.Database
		if tableDB == "" {
			tableDB = dbPrefix
		}

		parentDB := parentParsed.Database
		if parentDB == "" {
			parentDB = dbPrefix
		}

		r.Table = &Table{
			Name: StandardizeTableName(r.Table.Name, tableDB, schemaName, useBrackets),
		}
		r.ParentTable = &Table{
			Name: StandardizeTableName(r.ParentTable.Name, parentDB, schemaName, useBrackets),
		}

		updated[i] = &r
	}

	return updated
}

// updateFunctions updates function names with standardized format
func updateFunctions(functions []*Function, dbPrefix, schemaName string, useBrackets bool) []*Function {
	updated := make([]*Function, len(functions))

	for i, function := range functions {
		f := *function
		f.Name = StandardizeTableName(f.Name, dbPrefix, schemaName, useBrackets)
		updated[i] = &f
	}

	return updated
}

// ValidateMergedSchema validates the merged schema and reports on potential issues
func ValidateMergedSchema(schema *Schema) map[string]interface{} {
	results := map[string]interface{}{
		"total_tables":      len(schema.Tables),
		"total_relations":   len(schema.Relations),
		"virtual_relations": 0,
		"fk_relations":      0,
		"broken_relations":  []map[string]interface{}{},
		"missing_tables":    []string{},
		"databases":         []string{},
	}

	// Build table index
	tableNames := make(map[string]bool)
	databases := make(map[string]bool)

	for _, table := range schema.Tables {
		tableNames[table.Name] = true

		// Extract database name
		if db := ParseQualifiedName(table.Name).Database; db != "" {
			databases[db] = true
		}
	}

	// Count relation types
	for _, relation := range schema.Relations {
		if relation.Virtual {
			results["virtual_relations"] = results["virtual_relations"].(int) + 1
		} else {
			results["fk_relations"] = results["fk_relations"].(int) + 1
		}
	}

	// Check relations
	missingTables := make(map[string]bool)
	brokenRelations := []map[string]interface{}{}

	for _, relation := range schema.Relations {
		table := relation.Table.Name
		parent := relation.ParentTable.Name

		// Extract database names
		if db := ParseQualifiedName(table).Database; db != "" {
			databases[db] = true
		}
		if db := ParseQualifiedName(parent).Database; db != "" {
			databases[db] = true
		}

		// Check if tables exist
		if !tableNames[table] {
			missingTables[table] = true
			brokenRelations = append(brokenRelations, map[string]interface{}{
				"relation": fmt.Sprintf("%s -> %s", table, parent),
				"missing":  table,
				"virtual":  relation.Virtual,
			})
		}

		if !tableNames[parent] {
			missingTables[parent] = true
			brokenRelations = append(brokenRelations, map[string]interface{}{
				"relation": fmt.Sprintf("%s -> %s", table, parent),
				"missing":  parent,
				"virtual":  relation.Virtual,
			})
		}
	}

	// Convert maps to slices
	dbList := make([]string, 0, len(databases))
	for db := range databases {
		dbList = append(dbList, db)
	}
	results["databases"] = dbList

	missingList := make([]string, 0, len(missingTables))
	for table := range missingTables {
		missingList = append(missingList, table)
	}
	results["missing_tables"] = missingList
	results["broken_relations"] = brokenRelations

	return results
}
