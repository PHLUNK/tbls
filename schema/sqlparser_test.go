package schema

import (
	"strings"
	"testing"
)

func TestExtractJoinsFromSQL(t *testing.T) {
	tests := []struct {
		name          string
		sqlDef        string
		sourceTable   string
		defaultDB     string
		defaultSchema string
		useBrackets   bool
		expectedCount int
		checkFirst    func(*testing.T, *JoinRelation)
	}{
		{
			name: "simple inner join",
			sqlDef: `
				SELECT * FROM Orders o
				INNER JOIN Customers c ON o.customer_id = c.id
			`,
			sourceTable:   "Orders",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expectedCount: 1,
			checkFirst: func(t *testing.T, rel *JoinRelation) {
				if rel.FromTable != "Orders" {
					t.Errorf("FromTable: got %q, want %q", rel.FromTable, "Orders")
				}
				if rel.ToTable != "[DV].[dbo].[Customers]" {
					t.Errorf("ToTable: got %q, want %q", rel.ToTable, "[DV].[dbo].[Customers]")
				}
				if len(rel.FromColumns) != 1 || rel.FromColumns[0] != "customer_id" {
					t.Errorf("FromColumns: got %v, want [customer_id]", rel.FromColumns)
				}
				if len(rel.ToColumns) != 1 || rel.ToColumns[0] != "id" {
					t.Errorf("ToColumns: got %v, want [id]", rel.ToColumns)
				}
				if rel.JoinType != "INNER" {
					t.Errorf("JoinType: got %q, want %q", rel.JoinType, "INNER")
				}
			},
		},
		{
			name: "left join",
			sqlDef: `
				SELECT * FROM Orders o
				LEFT JOIN Customers c ON o.customer_id = c.id
			`,
			sourceTable:   "Orders",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expectedCount: 1,
			checkFirst: func(t *testing.T, rel *JoinRelation) {
				if rel.JoinType != "LEFT" {
					t.Errorf("JoinType: got %q, want %q", rel.JoinType, "LEFT")
				}
			},
		},
		{
			name: "right outer join",
			sqlDef: `
				SELECT * FROM Orders o
				RIGHT OUTER JOIN Customers c ON o.customer_id = c.id
			`,
			sourceTable:   "Orders",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expectedCount: 1,
			checkFirst: func(t *testing.T, rel *JoinRelation) {
				if rel.JoinType != "RIGHT" {
					t.Errorf("JoinType: got %q, want %q", rel.JoinType, "RIGHT")
				}
			},
		},
		{
			name: "join with schema-qualified table",
			sqlDef: `
				SELECT * FROM Orders o
				JOIN sales.Customers c ON o.customer_id = c.id
			`,
			sourceTable:   "Orders",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expectedCount: 1,
			checkFirst: func(t *testing.T, rel *JoinRelation) {
				if rel.ToTable != "[DV].[sales].[Customers]" {
					t.Errorf("ToTable: got %q, want %q", rel.ToTable, "[DV].[sales].[Customers]")
				}
			},
		},
		{
			name: "join with bracketed table names",
			sqlDef: `
				SELECT * FROM [Orders] o
				JOIN [dbo].[Customers] c ON o.[customer_id] = c.[id]
			`,
			sourceTable:   "Orders",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expectedCount: 1,
			checkFirst: func(t *testing.T, rel *JoinRelation) {
				if rel.ToTable != "[DV].[dbo].[Customers]" {
					t.Errorf("ToTable: got %q, want %q", rel.ToTable, "[DV].[dbo].[Customers]")
				}
				if len(rel.FromColumns) != 1 || rel.FromColumns[0] != "customer_id" {
					t.Errorf("FromColumns: got %v, want [customer_id]", rel.FromColumns)
				}
			},
		},
		{
			name: "multiple joins",
			sqlDef: `
				SELECT * FROM Orders o
				INNER JOIN Customers c ON o.customer_id = c.id
				LEFT JOIN Products p ON o.product_id = p.id
			`,
			sourceTable:   "Orders",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expectedCount: 2,
		},
		{
			name: "no joins",
			sqlDef: `
				SELECT * FROM Orders WHERE status = 'active'
			`,
			sourceTable:   "Orders",
			defaultDB:     "DV",
			defaultSchema: "dbo",
			useBrackets:   true,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			relations := ExtractJoinsFromSQL(tt.sqlDef, tt.sourceTable, tt.defaultDB, tt.defaultSchema, tt.useBrackets)

			if len(relations) != tt.expectedCount {
				t.Errorf("expected %d relations, got %d", tt.expectedCount, len(relations))
			}

			if tt.checkFirst != nil && len(relations) > 0 {
				tt.checkFirst(t, relations[0])
			}
		})
	}
}

func TestExtractRelationsFromDefinitions(t *testing.T) {
	tables := []*Table{
		{
			Name: "CustomerOrders",
			Type: "VIEW",
			Def: `
				CREATE VIEW CustomerOrders AS
				SELECT o.*, c.name
				FROM Orders o
				INNER JOIN Customers c ON o.customer_id = c.id
			`,
		},
		{
			Name: "ProductSales",
			Type: "VIEW",
			Def: `
				CREATE VIEW ProductSales AS
				SELECT p.*, o.quantity
				FROM Products p
				LEFT JOIN OrderItems o ON p.id = o.product_id
			`,
		},
		{
			Name: "SimpleTable",
			Type: "BASE TABLE",
			Def:  "", // No definition, should be skipped
		},
	}

	relations := ExtractRelationsFromDefinitions(tables, "DV", "dbo", true)

	// Should extract 2 relations (one from each view)
	if len(relations) != 2 {
		t.Errorf("expected 2 relations, got %d", len(relations))
	}

	// Check that all extracted relations are marked as virtual
	for _, rel := range relations {
		if !rel.Virtual {
			t.Errorf("relation should be marked as virtual: %+v", rel)
		}
	}

	// Check first relation
	if len(relations) > 0 {
		rel := relations[0]
		if rel.Table.Name != "CustomerOrders" {
			t.Errorf("Table: got %q, want %q", rel.Table.Name, "CustomerOrders")
		}
		if !strings.Contains(rel.Def, "INNER JOIN") {
			t.Errorf("Def should contain 'INNER JOIN': %q", rel.Def)
		}
		if rel.Cardinality != ExactlyOne {
			t.Errorf("Cardinality: got %v, want %v", rel.Cardinality, ExactlyOne)
		}
	}

	// Check second relation (LEFT JOIN should have different cardinality)
	if len(relations) > 1 {
		rel := relations[1]
		if rel.Table.Name != "ProductSales" {
			t.Errorf("Table: got %q, want %q", rel.Table.Name, "ProductSales")
		}
		if rel.Cardinality != ZeroOrOne {
			t.Errorf("Cardinality: got %v, want %v", rel.Cardinality, ZeroOrOne)
		}
	}
}

func TestDeduplicateRelations(t *testing.T) {
	relations := []*Relation{
		{
			Table:         &Table{Name: "[DV].[dbo].[Orders]"},
			Columns:       []*Column{{Name: "customer_id"}},
			ParentTable:   &Table{Name: "[DV].[dbo].[Customers]"},
			ParentColumns: []*Column{{Name: "id"}},
			Virtual:       false, // FK constraint
		},
		{
			Table:         &Table{Name: "[DV].[dbo].[Orders]"},
			Columns:       []*Column{{Name: "customer_id"}},
			ParentTable:   &Table{Name: "[DV].[dbo].[Customers]"},
			ParentColumns: []*Column{{Name: "id"}},
			Virtual:       true, // Extracted from view
		},
		{
			Table:         &Table{Name: "[DV].[dbo].[Orders]"},
			Columns:       []*Column{{Name: "product_id"}},
			ParentTable:   &Table{Name: "[DV].[dbo].[Products]"},
			ParentColumns: []*Column{{Name: "id"}},
			Virtual:       true,
		},
	}

	deduplicated := DeduplicateRelations(relations)

	// Should have 2 relations (duplicate removed)
	if len(deduplicated) != 2 {
		t.Errorf("expected 2 relations after deduplication, got %d", len(deduplicated))
	}

	// The non-virtual relation should be kept
	hasNonVirtualCustomer := false
	hasVirtualCustomer := false
	for _, rel := range deduplicated {
		if rel.Table.Name == "[DV].[dbo].[Orders]" && rel.ParentTable.Name == "[DV].[dbo].[Customers]" {
			if !rel.Virtual {
				hasNonVirtualCustomer = true
			} else {
				hasVirtualCustomer = true
			}
		}
	}

	if !hasNonVirtualCustomer {
		t.Error("should keep non-virtual (FK) relation over virtual one")
	}
	if hasVirtualCustomer {
		t.Error("should remove virtual duplicate when FK exists")
	}
}
