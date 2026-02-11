# Schema Merging Feature

This document describes the new schema merging functionality added to tbls, which allows you to combine multiple database schemas into a single unified documentation with cross-database relation detection.

## Overview

The schema merging feature implements the functionality previously provided by a Python post-processing script. It natively supports:

- **Name Standardization**: Standardizes table names to `[Database].[Schema].[Table]` format (configurable)
- **Cross-Schema Relations**: Identifies and preserves foreign key relationships across different schemas
- **Virtual Relation Extraction**: Automatically extracts JOIN relationships from SQL view definitions
- **Relation Deduplication**: Removes duplicate relations, preferring explicit FK constraints over extracted relations

## CLI Command

### Basic Usage

```bash
# Merge two or more schema JSON files
tbls merge schema1.json schema2.json -o combined.json

# Merge with custom name and description
tbls merge dv_schema.json dm_schema.json \
  --name "Data Warehouse" \
  --desc "Complete documentation of DV and DM databases" \
  -o warehouse.json

# Disable view relation extraction
tbls merge *.json -o combined.json --extract-view-relations=false

# Use dot notation instead of brackets
tbls merge *.json -o combined.json --brackets=false

# Validate the merged schema
tbls merge *.json -o combined.json --validate
```

### Database Name Mapping

By default, database names are extracted from filenames (e.g., `dv_schema.json` → `DV`). You can override this with explicit mappings:

```bash
tbls merge file1.json file2.json \
  --db-mapping file1.json:DataVault \
  --db-mapping file2.json:DataMart \
  -o combined.json
```

### Command-Line Options

| Flag | Description | Default |
|------|-------------|---------|
| `-o, --output` | Output file path (required) | - |
| `--name` | Name for the merged schema | "Combined Schema" |
| `--desc` | Description for the merged schema | Auto-generated |
| `--default-schema` | Default schema name | "dbo" |
| `--brackets` | Use SQL Server bracket notation `[Database].[Schema].[Table]` | `true` |
| `--extract-view-relations` | Extract virtual relations from view JOIN clauses | `true` |
| `--validate` | Validate merged schema and report issues | `false` |
| `--db-mapping` | Database name mapping (`filepath:dbname`) | Auto-detected |

## How It Works

### 1. Name Standardization

Tables are standardized to include database, schema, and table name:

**Before:**
- `Hub_Customer` (from dv_schema.json)
- `Fact_Sales` (from dm_schema.json)

**After:**
- `[DV].[dbo].[Hub_Customer]`
- `[DM].[dbo].[Fact_Sales]`

### 2. Cross-Schema Relation Detection

Foreign key constraints are preserved and standardized:

```json
{
  "table": "[DM].[dbo].[Fact_Sales]",
  "columns": ["customer_key"],
  "parent_table": "[DV].[dbo].[Hub_Customer]",
  "parent_columns": ["customer_key"],
  "virtual": false
}
```

### 3. Virtual Relation Extraction

The merger automatically parses SQL view definitions to extract JOIN relationships:

**View Definition:**
```sql
CREATE VIEW CustomerOrders AS
SELECT o.*, c.customer_name
FROM Orders o
INNER JOIN Customers c ON o.customer_id = c.id
```

**Extracted Relation:**
```json
{
  "table": "CustomerOrders",
  "columns": ["customer_id"],
  "parent_table": "[DV].[dbo].[Customers]",
  "parent_columns": ["id"],
  "cardinality": "exactly_one",
  "def": "[INNER JOIN] o.customer_id = c.id",
  "virtual": true
}
```

**JOIN Type Mapping:**
- `INNER JOIN` → `cardinality: exactly_one`
- `LEFT JOIN` → `cardinality: zero_or_one`
- `RIGHT JOIN` → `cardinality: zero_or_more`
- `FULL OUTER JOIN` → `cardinality: zero_or_more`

### 4. Deduplication

When a relation exists both as a FK constraint and extracted from a view, the FK constraint is kept (marked `virtual: false`).

## Programmatic Usage

You can also use the merging functionality programmatically in Go code:

```go
package main

import (
    "fmt"
    "github.com/k1LoW/tbls/schema"
)

func main() {
    config := &schema.MergeConfig{
        Name:                 "My Database",
        Description:          "Combined database documentation",
        DefaultSchema:        "dbo",
        UseBrackets:          true,
        ExtractViewRelations: true,
        DatabaseMapping: map[string]string{
            "db1.json": "DB1",
            "db2.json": "DB2",
        },
    }

    files := []string{"db1.json", "db2.json"}

    merged, stats, err := schema.MergeSchemas(files, config)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Merged %d tables from %d databases\n",
        stats.TotalTables, len(stats.Databases))
    fmt.Printf("Extracted %d virtual relations from views\n",
        stats.ExtractedRelations)

    // Save the merged schema
    err = schema.SaveSchemaToJSON(merged, "output.json")
    if err != nil {
        panic(err)
    }
}
```

## Implementation Details

### New Files

1. **`schema/identifier.go`**: Name parsing and standardization functions
   - `ParseQualifiedName()`: Parse `database.schema.table` strings
   - `StandardizeTableName()`: Normalize table names with database/schema prefixes
   - `BracketIdentifier()`: Add SQL Server bracket notation
   - `ExtractDatabaseName()`: Extract database name from filename

2. **`schema/sqlparser.go`**: SQL parsing for virtual relation extraction
   - `ExtractJoinsFromSQL()`: Parse JOIN clauses from SQL
   - `ExtractRelationsFromDefinitions()`: Extract relations from view definitions
   - `DeduplicateRelations()`: Remove duplicate relations

3. **`schema/merge.go`**: Schema merging logic
   - `MergeSchemas()`: Main merging function
   - `LoadSchemaFromJSON()`: Load schema from JSON file
   - `SaveSchemaToJSON()`: Save schema to JSON file
   - `ValidateMergedSchema()`: Validate and report issues

4. **`cmd/merge.go`**: CLI command implementation

### Test Coverage

Comprehensive test coverage includes:
- `schema/identifier_test.go`: Name parsing and standardization
- `schema/sqlparser_test.go`: SQL parsing and relation extraction

Run tests:
```bash
go test ./schema -v
```

## Example Workflow

### Step 1: Generate Individual Schema Files

```bash
# Generate schema for each database
tbls doc mssql://server/DataVault ./docs/dv
tbls doc mssql://server/DataMart ./docs/dm
```

This creates:
- `docs/dv/schema.json`
- `docs/dm/schema.json`

### Step 2: Merge Schemas

```bash
tbls merge \
  docs/dv/schema.json \
  docs/dm/schema.json \
  --name "Enterprise Data Warehouse" \
  --desc "Combined documentation for Data Vault and Data Mart" \
  --db-mapping docs/dv/schema.json:DV \
  --db-mapping docs/dm/schema.json:DM \
  --validate \
  -o docs/combined_schema.json
```

Output:
```
Merging 2 schema files...
  [1/2] docs/dv/schema.json
  [2/2] docs/dm/schema.json

======================================================================
Merge Complete!
======================================================================
Databases merged: DM, DV
Total tables: 145 (42 views)
Total relations: 234
  - From foreign key constraints: 187
  - Extracted from view JOINs: 52
  - Duplicates removed: 5
  - Cross-database relations: 23
Total functions: 12
Bracket notation: true
Output written to: docs/combined_schema.json
======================================================================

======================================================================
Schema Validation Report
======================================================================
Total tables: 145
Total relations: 234
  - Foreign key constraints: 187
  - Virtual relations (extracted): 47
Databases found: DM, DV

✓ All relations are valid!
======================================================================
```

### Step 3: Generate Documentation from Merged Schema

```bash
# Use the merged schema as a data source
tbls doc json://docs/combined_schema.json ./docs/combined
```

This generates complete documentation with cross-database diagrams and relationships.

## Migration from Python Script

If you were using the Python post-processing script, migration is straightforward:

**Before (Python):**
```python
merge_tbls_schemas(
    json_files=["dv.json", "dm.json"],
    output_file="combined.json",
    combined_name="Data Warehouse",
    default_schema="dbo",
    use_brackets=True,
    extract_view_relations=True,
)
```

**After (Go/tbls native):**
```bash
tbls merge dv.json dm.json \
  --name "Data Warehouse" \
  --default-schema dbo \
  --brackets=true \
  --extract-view-relations=true \
  -o combined.json
```

## Benefits

1. **Native Implementation**: No external Python dependencies
2. **Better Performance**: Go implementation is faster than Python
3. **Type Safety**: Leverages Go's type system for correctness
4. **Integration**: Direct integration with tbls CLI and workflow
5. **Testing**: Comprehensive test coverage
6. **Maintainability**: Single codebase instead of separate script

## Limitations

- JOIN parsing requires table aliases (e.g., `JOIN Table t` not just `JOIN Table`)
- Complex CTEs and nested queries may not extract all relations
- Only processes standard SQL JOIN syntax
- Cross-database foreign keys must be documented as relations (most RDBMS don't support them natively)

## Future Enhancements

Potential improvements:
- Support for more complex SQL parsing (nested subqueries, CTEs)
- Alias-optional JOIN detection
- Relation confidence scoring
- Interactive relation validation
- Automatic schema comparison and diff
