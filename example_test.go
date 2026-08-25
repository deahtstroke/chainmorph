package chainmorph

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
)

// Custom made reader that reads from a file with location Path
type JsonReader[T any] struct {
	// The path of the file to read
	Path string
}

// JsonReader's ReadFrom() method  opens a file from using its path
// field and returns the specified type T after unmarhsalling
func (f *JsonReader[T]) ReadFrom(ctx context.Context) (T, error) {
	var zero T
	content, err := os.ReadFile(f.Path)
	if err != nil {
		return zero, err
	}

	var b T
	if err := json.Unmarshal(content, &b); err != nil {
		return zero, err
	}

	return b, nil
}

// StdoutWriter is a simple no-op writer that prints the result(s) to the console
type StdoutWriter[T any] struct{}

func (o *StdoutWriter[T]) WriteTo(ctx context.Context, item T) error {
	_, err := fmt.Printf("<Item>: %v\n", item)
	return err
}
