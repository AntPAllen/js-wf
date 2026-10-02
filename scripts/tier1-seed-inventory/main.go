// Emit the source inventory of seeded test loops, independent of test output.
package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func main() {
	files, err := filepath.Glob("sim/*_test.go")
	if err != nil {
		panic(err)
	}
	var names []string
	for _, path := range files {
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			panic(err)
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil || !strings.HasPrefix(fn.Name.Name, "Test") {
				continue
			}
			calls := 0
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if ok {
					ident, ok := call.Fun.(*ast.Ident)
					if ok && ident.Name == "seededSchedules" {
						calls++
					}
				}
				return true
			})
			if calls > 1 {
				fmt.Fprintln(os.Stderr, "ambiguous multiple seed loops:", fn.Name.Name)
				os.Exit(1)
			}
			if calls == 1 {
				names = append(names, fn.Name.Name)
			}
		}
	}
	if len(names) == 0 {
		panic("empty seeded test inventory")
	}
	sort.Strings(names)
	for _, name := range names {
		fmt.Println(name)
	}
}
