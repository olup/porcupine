package main

import "testing"

func TestInteractiveQueryEnvironment(t *testing.T) {
	names := []string{"PORCUPINE_QUERY_MAX_CONCURRENT", "PORCUPINE_QUERY_TIMEOUT_MS", "PORCUPINE_QUERY_MAX_ROWS", "PORCUPINE_QUERY_MAX_RESULT_BYTES"}
	for _, name := range names {
		t.Setenv(name, "")
	}
	c, err := (settings{}).interactiveQueryConfiguration()
	if err != nil || c.MaxConcurrent != 0 || c.TimeoutMS != 0 || c.MaxRows != 0 || c.MaxResultBytes != 0 {
		t.Fatal(c, err)
	}
	for i, name := range names {
		for _, bad := range []string{"-1", "0", "invalid", "9999999999999999999999"} {
			t.Setenv(name, bad)
			if _, err = (settings{}).interactiveQueryConfiguration(); err == nil {
				t.Fatal("invalid query configuration accepted", name, bad)
			}
		}
		values := []string{"4", "30000", "50000", "8388608"}
		t.Setenv(name, values[i])
	}
	c, err = (settings{}).interactiveQueryConfiguration()
	if err != nil || c.MaxConcurrent != 4 || c.TimeoutMS != 30000 || c.MaxRows != 50000 || c.MaxResultBytes != 8<<20 {
		t.Fatal(c, err)
	}
}
