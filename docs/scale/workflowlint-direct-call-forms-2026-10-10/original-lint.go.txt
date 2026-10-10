// Package workflowlint finds direct wall-clock and random calls inside Go
// workflow handlers. It is a source check, not a call-graph analysis.
package workflowlint

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Diagnostic struct {
	File   string
	Line   int
	Column int
	Call   string
	Hint   string
}

// Scan checks Go files and directories recursively. A workflow handler is a
// function or function literal with a *wf.Context parameter. Direct calls in
// its body and nested closures are checked; calls in separate helpers are not.
func Scan(paths ...string) ([]Diagnostic, error) {
	if len(paths) == 0 {
		paths = []string{"."}
	}
	files := map[string]struct{}{}
	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			if filepath.Ext(path) != ".go" {
				return nil, fmt.Errorf("not a Go file: %s", path)
			}
			files[path] = struct{}{}
			continue
		}
		err = filepath.WalkDir(path, func(name string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() && name != path && (strings.HasPrefix(entry.Name(), ".") || entry.Name() == "vendor") {
				return filepath.SkipDir
			}
			if !entry.IsDir() && filepath.Ext(name) == ".go" {
				files[name] = struct{}{}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	fset := token.NewFileSet()
	var findings []Diagnostic
	for name := range files {
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			return nil, err
		}
		findings = append(findings, scanFile(fset, file)...)
	}
	sort.Slice(findings, func(i, j int) bool {
		a, b := findings[i], findings[j]
		if a.File != b.File {
			return a.File < b.File
		}
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Column < b.Column
	})
	return findings, nil
}

func scanFile(fset *token.FileSet, file *ast.File) []Diagnostic {
	wfAliases := map[string]bool{}
	timeAliases := map[string]bool{}
	randAliases := map[string]bool{}
	for _, spec := range file.Imports {
		path, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			continue
		}
		alias := filepath.Base(path)
		if spec.Name != nil {
			alias = spec.Name.Name
		}
		if alias == "_" || alias == "." {
			continue
		}
		switch {
		case path == "js-wf/wf" || strings.HasSuffix(path, "/wf"):
			wfAliases[alias] = true
		case path == "time":
			timeAliases[alias] = true
		case path == "math/rand" || path == "math/rand/v2" || path == "crypto/rand":
			randAliases[alias] = true
		}
	}
	if len(wfAliases) == 0 || len(timeAliases)+len(randAliases) == 0 {
		return nil
	}
	seen := map[token.Pos]bool{}
	var findings []Diagnostic
	inspectHandler := func(body *ast.BlockStmt) {
		journaledEffects := map[token.Pos]bool{}
		ast.Inspect(body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok || len(call.Args) == 0 {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "Run" && selector.Sel.Name != "RunOnce" {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok || pkg.Obj != nil || !wfAliases[pkg.Name] {
				return true
			}
			if effect, ok := call.Args[len(call.Args)-1].(*ast.FuncLit); ok {
				journaledEffects[effect.Pos()] = true
			}
			return true
		})
		ast.Inspect(body, func(node ast.Node) bool {
			if closure, ok := node.(*ast.FuncLit); ok && journaledEffects[closure.Pos()] {
				return false
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			pkg, ok := selector.X.(*ast.Ident)
			if !ok || pkg.Obj != nil || seen[call.Pos()] {
				return true
			}
			hint := ""
			if timeAliases[pkg.Name] && selector.Sel.Name == "Now" {
				hint = "journal time with wf.Now"
			} else if randAliases[pkg.Name] {
				hint = "journal randomness with wf.Random or a recorded step"
			}
			if hint != "" {
				seen[call.Pos()] = true
				position := fset.Position(call.Pos())
				findings = append(findings, Diagnostic{File: position.Filename, Line: position.Line, Column: position.Column, Call: pkg.Name + "." + selector.Sel.Name, Hint: hint})
			}
			return true
		})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch fn := node.(type) {
		case *ast.FuncDecl:
			if fn.Body != nil && hasWorkflowContext(fn.Type.Params, wfAliases) {
				inspectHandler(fn.Body)
			}
		case *ast.FuncLit:
			if hasWorkflowContext(fn.Type.Params, wfAliases) {
				inspectHandler(fn.Body)
			}
		}
		return true
	})
	return findings
}

func hasWorkflowContext(params *ast.FieldList, aliases map[string]bool) bool {
	if params == nil {
		return false
	}
	for _, field := range params.List {
		pointer, ok := field.Type.(*ast.StarExpr)
		if !ok {
			continue
		}
		selector, ok := pointer.X.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "Context" {
			continue
		}
		pkg, ok := selector.X.(*ast.Ident)
		if ok && pkg.Obj == nil && aliases[pkg.Name] {
			return true
		}
	}
	return false
}
