// Copyright © 2018 Ken'ichiro Oyama <k1lowxb@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"fmt"
	sortpkg "sort"
	"strings"

	"github.com/k1LoW/errors"
	"github.com/k1LoW/tbls/cmdutil"
	"github.com/k1LoW/tbls/schema"
	"github.com/spf13/cobra"
)

var (
	outputFile           string
	mergedName           string
	mergedDesc           string
	defaultSchema        string
	useBrackets          bool
	extractViewRelations bool
	validate             bool
	dbMappings           []string // Format: "filepath:dbname"
)

// mergeCmd represents the merge command
var mergeCmd = &cobra.Command{
	Use:   "merge [JSON_FILE1] [JSON_FILE2] ...",
	Short: "merge multiple tbls schema JSON files",
	Long: `'tbls merge' merges multiple tbls schema JSON files into a single combined schema.
This is useful for documenting multi-database systems with cross-database relations.

The merge command:
- Standardizes table names with database.schema.table format
- Extracts virtual relations from view JOIN clauses
- Identifies cross-schema and cross-database relationships
- Deduplicates relations (preferring FK constraints over extracted relations)

Example:
  tbls merge dv_schema.json dm_schema.json -o combined.json
  tbls merge *.json --name "Data Warehouse" --extract-view-relations
  tbls merge db1.json db2.json --db-mapping db1.json:DV --db-mapping db2.json:DM`,
	RunE: func(_ *cobra.Command, args []string) error {
		if allow, err := cmdutil.IsAllowedToExecute(when); !allow || err != nil {
			if err != nil {
				return err
			}
			return nil
		}

		if len(args) < 2 {
			return errors.WithStack(errors.New("at least 2 JSON files are required"))
		}

		if outputFile == "" {
			return errors.WithStack(errors.New("output file must be specified with -o or --output"))
		}

		// Parse database mappings
		dbMapping := make(map[string]string)
		for _, mapping := range dbMappings {
			parts := strings.SplitN(mapping, ":", 2)
			if len(parts) != 2 {
				return errors.WithStack(fmt.Errorf("invalid database mapping format: %s (expected filepath:dbname)", mapping))
			}
			dbMapping[parts[0]] = parts[1]
		}

		// Create merge configuration
		config := &schema.MergeConfig{
			Name:                 mergedName,
			Description:          mergedDesc,
			DefaultSchema:        defaultSchema,
			UseBrackets:          useBrackets,
			ExtractViewRelations: extractViewRelations,
			DatabaseMapping:      dbMapping,
		}

		if config.Name == "" {
			config.Name = "Combined Schema"
		}

		if config.Description == "" {
			config.Description = fmt.Sprintf("Combined schema from %d databases", len(args))
		}

		// Merge schemas
		fmt.Printf("Merging %d schema files...\n", len(args))
		for i, file := range args {
			fmt.Printf("  [%d/%d] %s\n", i+1, len(args), file)
		}

		merged, stats, err := schema.MergeSchemas(args, config)
		if err != nil {
			return errors.WithStack(err)
		}

		// Save merged schema
		if err := schema.SaveSchemaToJSON(merged, outputFile); err != nil {
			return errors.WithStack(err)
		}

		// Print summary
		printMergeSummary(stats, merged, outputFile)

		// Validate if requested
		if validate {
			fmt.Println()
			printValidationReport(merged)
		}

		return nil
	},
}

func printMergeSummary(stats *schema.MergeStats, merged *schema.Schema, outputFile string) {
	fmt.Println()
	fmt.Println(strings.Repeat("=", 70))
	fmt.Println("Merge Complete!")
	fmt.Println(strings.Repeat("=", 70))

	// Sort databases for consistent output
	databases := make([]string, len(stats.Databases))
	copy(databases, stats.Databases)
	sortpkg.Strings(databases)

	fmt.Printf("Databases merged: %s\n", strings.Join(databases, ", "))
	fmt.Printf("Total tables: %d (%d views)\n", stats.TotalTables, stats.TotalViews)
	fmt.Printf("Total relations: %d\n", len(merged.Relations))
	fmt.Printf("  - From foreign key constraints: %d\n", stats.TotalRelations)
	if extractViewRelations {
		fmt.Printf("  - Extracted from view JOINs: %d\n", stats.ExtractedRelations)
	}
	if stats.DeduplicatedCount > 0 {
		fmt.Printf("  - Duplicates removed: %d\n", stats.DeduplicatedCount)
	}
	if stats.CrossDBRelations > 0 {
		fmt.Printf("  - Cross-database relations: %d\n", stats.CrossDBRelations)
	}
	fmt.Printf("Total functions: %d\n", stats.TotalFunctions)
	fmt.Printf("Bracket notation: %v\n", useBrackets)
	fmt.Printf("Output written to: %s\n", outputFile)
	fmt.Println(strings.Repeat("=", 70))
}

func printValidationReport(merged *schema.Schema) {
	results := schema.ValidateMergedSchema(merged)

	fmt.Println(strings.Repeat("=", 70))
	fmt.Println("Schema Validation Report")
	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("Total tables: %d\n", results["total_tables"])
	fmt.Printf("Total relations: %d\n", results["total_relations"])
	fmt.Printf("  - Foreign key constraints: %d\n", results["fk_relations"])
	fmt.Printf("  - Virtual relations (extracted): %d\n", results["virtual_relations"])

	databases := results["databases"].([]string)
	sortpkg.Strings(databases)
	fmt.Printf("Databases found: %s\n", strings.Join(databases, ", "))

	brokenRelations := results["broken_relations"].([]map[string]interface{})
	if len(brokenRelations) > 0 {
		fmt.Printf("\n⚠️  Found %d broken relations:\n", len(brokenRelations))

		// Show first 10
		limit := 10
		if len(brokenRelations) < limit {
			limit = len(brokenRelations)
		}

		for i := 0; i < limit; i++ {
			broken := brokenRelations[i]
			relType := "FK"
			if virtual, ok := broken["virtual"].(bool); ok && virtual {
				relType = "Virtual"
			}
			fmt.Printf("  [%s] %s\n", relType, broken["relation"])
			fmt.Printf("       Missing: %s\n", broken["missing"])
		}

		if len(brokenRelations) > limit {
			fmt.Printf("  ... and %d more\n", len(brokenRelations)-limit)
		}
	} else {
		fmt.Println("\n✓ All relations are valid!")
	}

	fmt.Println(strings.Repeat("=", 70))
}

func init() {
	mergeCmd.Flags().StringVarP(&outputFile, "output", "o", "", "output file path (required)")
	mergeCmd.Flags().StringVar(&mergedName, "name", "", "name for the merged schema")
	mergeCmd.Flags().StringVar(&mergedDesc, "desc", "", "description for the merged schema")
	mergeCmd.Flags().StringVar(&defaultSchema, "default-schema", "dbo", "default schema name")
	mergeCmd.Flags().BoolVar(&useBrackets, "brackets", true, "use SQL Server bracket notation [Database].[Schema].[Table]")
	mergeCmd.Flags().BoolVar(&extractViewRelations, "extract-view-relations", true, "extract virtual relations from view JOIN clauses")
	mergeCmd.Flags().BoolVar(&validate, "validate", false, "validate merged schema and report issues")
	mergeCmd.Flags().StringArrayVar(&dbMappings, "db-mapping", []string{}, "database name mapping in format filepath:dbname")
	mergeCmd.Flags().StringVarP(&when, "when", "", "", "command execute condition")

	_ = mergeCmd.MarkFlagRequired("output")

	rootCmd.AddCommand(mergeCmd)
}
