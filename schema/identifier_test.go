package schema

import (
	"testing"
)

func TestParseQualifiedName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected QualifiedName
	}{
		{
			name:  "table only",
			input: "Users",
			expected: QualifiedName{
				Database: "",
				Schema:   "",
				Table:    "Users",
			},
		},
		{
			name:  "schema.table",
			input: "dbo.Users",
			expected: QualifiedName{
				Database: "",
				Schema:   "dbo",
				Table:    "Users",
			},
		},
		{
			name:  "database.schema.table",
			input: "DV.dbo.Hub_Customer",
			expected: QualifiedName{
				Database: "DV",
				Schema:   "dbo",
				Table:    "Hub_Customer",
			},
		},
		{
			name:  "bracketed table",
			input: "[Users]",
			expected: QualifiedName{
				Database: "",
				Schema:   "",
				Table:    "Users",
			},
		},
		{
			name:  "bracketed schema.table",
			input: "[dbo].[Users]",
			expected: QualifiedName{
				Database: "",
				Schema:   "dbo",
				Table:    "Users",
			},
		},
		{
			name:  "bracketed database.schema.table",
			input: "[DV].[dbo].[Hub_Customer]",
			expected: QualifiedName{
				Database: "DV",
				Schema:   "dbo",
				Table:    "Hub_Customer",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParseQualifiedName(tt.input)
			if result.Database != tt.expected.Database {
				t.Errorf("Database: got %q, want %q", result.Database, tt.expected.Database)
			}
			if result.Schema != tt.expected.Schema {
				t.Errorf("Schema: got %q, want %q", result.Schema, tt.expected.Schema)
			}
			if result.Table != tt.expected.Table {
				t.Errorf("Table: got %q, want %q", result.Table, tt.expected.Table)
			}
		})
	}
}

func TestNormalizeBrackets(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "no brackets",
			input:    "Users",
			expected: "Users",
		},
		{
			name:     "with brackets",
			input:    "[Users]",
			expected: "Users",
		},
		{
			name:     "qualified with brackets",
			input:    "[DV].[dbo].[Users]",
			expected: "DV.dbo.Users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeBrackets(tt.input)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBracketIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "Users",
			expected: "[Users]",
		},
		{
			name:     "already bracketed",
			input:    "[Users]",
			expected: "[Users]",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "with spaces",
			input:    "  Table  ",
			expected: "[Table]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BracketIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestBuildQualifiedName(t *testing.T) {
	tests := []struct {
		name        string
		table       string
		schema      string
		database    string
		useBrackets bool
		expected    string
	}{
		{
			name:        "table only, no brackets",
			table:       "Users",
			schema:      "",
			database:    "",
			useBrackets: false,
			expected:    "Users",
		},
		{
			name:        "table only, with brackets",
			table:       "Users",
			schema:      "",
			database:    "",
			useBrackets: true,
			expected:    "[Users]",
		},
		{
			name:        "schema.table, no brackets",
			table:       "Users",
			schema:      "dbo",
			database:    "",
			useBrackets: false,
			expected:    "dbo.Users",
		},
		{
			name:        "schema.table, with brackets",
			table:       "Users",
			schema:      "dbo",
			database:    "",
			useBrackets: true,
			expected:    "[dbo].[Users]",
		},
		{
			name:        "database.schema.table, no brackets",
			table:       "Hub_Customer",
			schema:      "dbo",
			database:    "DV",
			useBrackets: false,
			expected:    "DV.dbo.Hub_Customer",
		},
		{
			name:        "database.schema.table, with brackets",
			table:       "Hub_Customer",
			schema:      "dbo",
			database:    "DV",
			useBrackets: true,
			expected:    "[DV].[dbo].[Hub_Customer]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildQualifiedName(tt.table, tt.schema, tt.database, tt.useBrackets)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestStandardizeTableName(t *testing.T) {
	tests := []struct {
		name          string
		tableName     string
		defaultDB     string
		defaultSchema string
		useBrackets   bool
		expected      string
	}{
		{
			name:          "table only, add defaults",
			tableName:     "Users",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expected:      "[DV].[dbo].[Users]",
		},
		{
			name:          "schema.table, add database",
			tableName:     "sales.Orders",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expected:      "[DV].[sales].[Orders]",
		},
		{
			name:          "full qualified, no defaults needed",
			tableName:     "DM.reporting.Summary",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expected:      "[DM].[reporting].[Summary]",
		},
		{
			name:          "bracketed input",
			tableName:     "[dbo].[Users]",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expected:      "[DV].[dbo].[Users]",
		},
		{
			name:          "no brackets mode",
			tableName:     "Users",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   false,
			expected:      "DV.dbo.Users",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := StandardizeTableName(tt.tableName, tt.defaultDB, tt.defaultSchema, tt.useBrackets)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

func TestExtractDatabaseName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "underscore suffix",
			input:    "dv_schema.json",
			expected: "DV",
		},
		{
			name:     "hyphen suffix",
			input:    "dv-schema.json",
			expected: "DV",
		},
		{
			name:     "no suffix",
			input:    "datamart.json",
			expected: "DATAMART",
		},
		{
			name:     "with path",
			input:    "/path/to/dm_schema.json",
			expected: "DM",
		},
		{
			name:     "windows path",
			input:    "C:\\schemas\\sa_schema.json",
			expected: "SA",
		},
		{
			name:     "multi-part name",
			input:    "my-database-schema.json",
			expected: "MY-DATABASE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractDatabaseName(tt.input)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}
