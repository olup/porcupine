// Command port-control-sql converts literal database/sql bind markers to the
// numbered form supported by both SQLite and PostgreSQL. It never runs in the server.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
)

func main() {
	flag.Parse()
	for _, path := range flag.Args() {
		if err := rewriteFile(path); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}

func rewriteFile(path string) error {
	set := token.NewFileSet()
	f, err := parser.ParseFile(set, path, nil, parser.ParseComments)
	if err != nil {
		return err
	}
	changed := false
	ast.Inspect(f, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		arg := 0
		switch method.Sel.Name {
		case "ExecContext", "QueryContext", "QueryRowContext", "PrepareContext":
			arg = 1
		case "Exec", "Query", "QueryRow", "Prepare":
		default:
			return true
		}
		if len(call.Args) <= arg {
			return true
		}
		next := 1
		var visit func(ast.Expr)
		visit = func(expr ast.Expr) {
			switch value := expr.(type) {
			case *ast.BinaryExpr:
				if value.Op == token.ADD {
					visit(value.X)
					visit(value.Y)
				}
			case *ast.BasicLit:
				if value.Kind != token.STRING {
					return
				}
				before, err := strconv.Unquote(value.Value)
				if err != nil {
					return
				}
				after := numbered(before, &next)
				if before != after {
					changed = true
					if strings.HasPrefix(value.Value, "`") {
						value.Value = "`" + after + "`"
					} else {
						value.Value = strconv.Quote(after)
					}
				}
			}
		}
		visit(call.Args[arg])
		return true
	})
	if !changed {
		return nil
	}
	var output bytes.Buffer
	if err := format.Node(&output, set, f); err != nil {
		return err
	}
	return os.WriteFile(path, output.Bytes(), 0644)
}

func numbered(input string, next *int) string {
	var out strings.Builder
	var quote byte
	for i := 0; i < len(input); i++ {
		c := input[i]
		if quote != 0 {
			out.WriteByte(c)
			if c == quote {
				if i+1 < len(input) && input[i+1] == quote {
					i++
					out.WriteByte(input[i])
				} else {
					quote = 0
				}
			}
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			quote = c
			out.WriteByte(c)
			continue
		}
		if c != '?' {
			out.WriteByte(c)
			continue
		}
		j := i + 1
		for j < len(input) && input[j] >= '0' && input[j] <= '9' {
			j++
		}
		index := *next
		if j > i+1 {
			index, _ = strconv.Atoi(input[i+1 : j])
		}
		if index >= *next {
			*next = index + 1
		}
		out.WriteString("$" + strconv.Itoa(index))
		i = j - 1
	}
	return out.String()
}
