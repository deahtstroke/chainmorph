package chainmorph

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"testing"
)

type spyWriter[T any] struct {
	items []T
}

func (w *spyWriter[T]) Write(ctx context.Context, item T) error {
	w.items = append(w.items, item)
	return nil
}

func (w *spyWriter[T]) all() []T {
	return w.items
}

func TestReadAndWriteSuccess(t *testing.T) {
	source := []int{1, 2, 3, 4}
	spyWriter := &spyWriter[int]{}
	ctx := context.Background()
	err := Just(source...).WriteTo(ctx, spyWriter)

	want := source
	got := spyWriter.all()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !slices.Equal(want, got) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFilterIfSuccess(t *testing.T) {
	source := []int{1, 2, 3, 4, 5, 6, 7, 8}
	spyWriter := &spyWriter[int]{}
	ctx := context.Background()
	err := Just(source...).
		If(func(item int) bool {
			return item%2 == 0
		}).
		WriteTo(ctx, spyWriter)

	want := []int{2, 4, 6, 8}
	got := spyWriter.all()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !slices.Equal(want, got) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFilterFilterFunc(t *testing.T) {
	source := []string{"A", "B", "C", "AB", "AC", "AD"}
	spyWriter := &spyWriter[string]{}
	ctx := context.Background()
	err := Just(source...).
		FilterFunc(func(ctx context.Context, s string) (bool, error) {
			return !strings.HasPrefix(s, "A"), nil
		}).
		WriteTo(ctx, spyWriter)

	want := []string{"B", "C"}
	got := spyWriter.all()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !slices.Equal(want, got) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestFilterFilterFuncError(t *testing.T) {
	source := []string{"A", "B", "C", "AB", "AC", "AD"}
	spyWriter := &spyWriter[string]{}
	ctx := context.Background()
	err := Just(source...).
		FilterFunc(func(ctx context.Context, s string) (bool, error) {
			i, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				return false, err
			}
			return i > 0, nil
		}).
		WriteTo(ctx, spyWriter)

	if err == nil {
		t.Fatal("expecting error, got none")
	}
}
