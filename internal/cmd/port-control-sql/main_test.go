package main

import "testing"

func TestNumberedBindings(t *testing.T) {
	for _, tc := range []struct{ before, after string }{
		{`SELECT ?,'?',"?",?,?1,?2`, `SELECT $1,'?',"?",$2,$1,$2`},
		{`UPDATE x SET v=? WHERE id=?`, `UPDATE x SET v=$1 WHERE id=$2`},
		{`SELECT 'it''s ?' WHERE x=$1`, `SELECT 'it''s ?' WHERE x=$1`},
	} {
		next := 1
		if got := numbered(tc.before, &next); got != tc.after {
			t.Fatalf("%q: %q", tc.before, got)
		}
	}
}
