package porcupine

import (
	"context"
	"errors"
	"io"
	"os"
)

func copyCheckpointFile(ctx context.Context, source, destination string) (result error) {
	input, err := os.Open(source)
	if err != nil {
		return err
	}
	defer input.Close()
	output, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, output.Close()) }()
	buffer := make([]byte, 128<<10)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := input.Read(buffer)
		if n > 0 {
			if _, writeErr := output.Write(buffer[:n]); writeErr != nil {
				return writeErr
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
	}
	return output.Sync()
}
