package schema

import (
	"regexp"
	"strings"
)

// JoinRelation represents a relation discovered from SQL JOIN clauses
type JoinRelation struct {
	FromTable     string
	FromColumns   []string
	ToTable       string
	ToColumns     []string
	JoinType      string
	OnCondition   string
}

// ExtractJoinsFromSQL extracts JOIN relationships from SQL definitions (views, procedures).
// Returns list of discovered relations with their join columns.
func ExtractJoinsFromSQL(sqlDef, sourceTable, defaultDB, defaultSchema string, useBrackets bool) []*JoinRelation {
	if sqlDef == "" {
		return nil
	}

	var relations []*JoinRelation

	// Pattern to match various JOIN syntax
	// Matches: LEFT JOIN, INNER JOIN, RIGHT JOIN, JOIN, etc.
	// Captures: table name, optional alias, and ON condition
	joinPattern := regexp.MustCompile(
		`(?i)(?:LEFT\s+|RIGHT\s+|INNER\s+|OUTER\s+|CROSS\s+)?(?:OUTER\s+)?JOIN\s+` +
			`([\[\w\]\.]+)` + // table name
			`\s+(?:AS\s+)?([\w]+)` + // alias (simplified - required for easier parsing)
			`\s+ON\s+([^;]+?)` + // ON condition
			`(?:\s+WHERE|\s+GROUP|\s+ORDER|\s+HAVING|\s+UNION|\s+LEFT|\s+RIGHT|\s+INNER|\s+JOIN|;|\s*$)`, // terminators
	)

	matches := joinPattern.FindAllStringSubmatch(sqlDef, -1)

	for _, match := range matches {
		if len(match) < 4 {
			continue
		}

		joinedTable := strings.TrimSpace(match[1])
		// alias := strings.TrimSpace(match[2]) // Not currently used but available
		onCondition := strings.TrimSpace(match[3])

		// Standardize the joined table name
		joinedTableStd := StandardizeTableName(joinedTable, defaultDB, defaultSchema, useBrackets)

		// Parse the ON condition to extract columns
		// Pattern: alias.column = other_alias.column OR table.column = table.column
		columnPattern := regexp.MustCompile(`(\[?[\w]+\]?)\.(\[?\w+\]?)\s*=\s*(\[?[\w]+\]?)\.(\[?\w+\]?)`)
		colMatches := columnPattern.FindAllStringSubmatch(onCondition, -1)

		for _, colMatch := range colMatches {
			if len(colMatch) < 5 {
				continue
			}

			// leftAlias := NormalizeBrackets(strings.TrimSpace(colMatch[1]))
			leftColumn := NormalizeBrackets(strings.TrimSpace(colMatch[2]))
			// rightAlias := NormalizeBrackets(strings.TrimSpace(colMatch[3]))
			rightColumn := NormalizeBrackets(strings.TrimSpace(colMatch[4]))

			// Determine join type
			joinType := "INNER"
			matchUpper := strings.ToUpper(match[0])
			if strings.Contains(matchUpper, "LEFT") {
				joinType = "LEFT"
			} else if strings.Contains(matchUpper, "RIGHT") {
				joinType = "RIGHT"
			} else if strings.Contains(matchUpper, "FULL") {
				joinType = "FULL"
			}

			// Clean up ON condition for display
			cleanCondition := strings.ReplaceAll(strings.ReplaceAll(onCondition, "\n", " "), "\r", "")
			cleanCondition = strings.TrimSpace(regexp.MustCompile(`\s+`).ReplaceAllString(cleanCondition, " "))

			relations = append(relations, &JoinRelation{
				FromTable:   sourceTable,
				FromColumns: []string{leftColumn},
				ToTable:     joinedTableStd,
				ToColumns:   []string{rightColumn},
				JoinType:    joinType,
				OnCondition: cleanCondition,
			})
		}
	}

	return relations
}

// ExtractRelationsFromDefinitions extracts virtual relations from view and procedure definitions.
func ExtractRelationsFromDefinitions(tables []*Table, defaultDB, defaultSchema string, useBrackets bool) []*Relation {
	var virtualRelations []*Relation

	for _, table := range tables {
		// Only process views and tables with SQL definitions
		if table.Type != "VIEW" && table.Type != "MATERIALIZED VIEW" && table.Type != "BASE TABLE" {
			continue
		}

		if table.Def == "" {
			continue
		}

		tableName := table.Name

		// Extract joins from this view
		discoveredJoins := ExtractJoinsFromSQL(table.Def, tableName, defaultDB, defaultSchema, useBrackets)

		for _, joinInfo := range discoveredJoins {
			// Map join type to cardinality
			var cardinality, parentCardinality Cardinality
			switch joinInfo.JoinType {
			case "LEFT":
				cardinality = ZeroOrOne
				parentCardinality = ZeroOrMore
			case "RIGHT":
				cardinality = ZeroOrMore
				parentCardinality = ZeroOrOne
			default: // INNER, FULL, etc.
				cardinality = ExactlyOne
				parentCardinality = ZeroOrMore
			}

			// Truncate condition for display
			def := joinInfo.OnCondition
			if len(def) > 100 {
				def = def[:100] + "..."
			}
			def = "[" + joinInfo.JoinType + " JOIN] " + def

			// Convert string column names to Column objects
			columns := make([]*Column, len(joinInfo.FromColumns))
			for i, col := range joinInfo.FromColumns {
				columns[i] = &Column{Name: col}
			}

			parentColumns := make([]*Column, len(joinInfo.ToColumns))
			for i, col := range joinInfo.ToColumns {
				parentColumns[i] = &Column{Name: col}
			}

			virtualRelation := &Relation{
				Table:             &Table{Name: joinInfo.FromTable},
				Columns:           columns,
				Cardinality:       cardinality,
				ParentTable:       &Table{Name: joinInfo.ToTable},
				ParentColumns:     parentColumns,
				ParentCardinality: parentCardinality,
				Def:               def,
				Virtual:           true,
			}

			virtualRelations = append(virtualRelations, virtualRelation)
		}
	}

	return virtualRelations
}

// DeduplicateRelations removes duplicate relations based on table, columns, and parent_table.
// Prefers non-virtual relations over virtual ones.
func DeduplicateRelations(relations []*Relation) []*Relation {
	type relKey struct {
		table       string
		columns     string
		parentTable string
		parentCols  string
	}

	seen := make(map[relKey]*Relation)

	for _, relation := range relations {
		// Convert column slices to strings for comparison
		colNames := make([]string, len(relation.Columns))
		for i, col := range relation.Columns {
			colNames[i] = col.Name
		}

		parentColNames := make([]string, len(relation.ParentColumns))
		for i, col := range relation.ParentColumns {
			parentColNames[i] = col.Name
		}

		// Create a key for deduplication
		key := relKey{
			table:       relation.Table.Name,
			columns:     strings.Join(colNames, ","),
			parentTable: relation.ParentTable.Name,
			parentCols:  strings.Join(parentColNames, ","),
		}

		existing, exists := seen[key]
		if !exists {
			seen[key] = relation
		} else {
			// If current relation is not virtual but existing is, replace
			if !relation.Virtual && existing.Virtual {
				seen[key] = relation
			}
		}
	}

	// Convert map back to slice
	result := make([]*Relation, 0, len(seen))
	for _, rel := range seen {
		result = append(result, rel)
	}

	return result
}
