package porcupine

type ImportRowResult struct {
	Accepted   int64
	Duplicates int64
	Ignored    int64
	Unresolved int64
}
