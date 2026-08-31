package chainmorph_test

import (
	"context"
	"fmt"

	"github.com/deahtstroke/chainmorph"
)

type user struct {
	Name        string
	Department  string
	BadgeNumber int
}

type sliceReader[T any] struct {
	data []T
}

func (r *sliceReader[T]) ReadFrom(ctx context.Context) (T, error) {
	var zero T
	if len(r.data) <= 0 {
		return zero, chainmorph.ErrEndOfStream
	}

	var item T
	item, r.data = r.data[0], r.data[1:]
	return item, nil
}

type stdWriter[T any] struct{}

func (w *stdWriter[T]) Write(ctx context.Context, item T) error {
	fmt.Println(item)
	return nil
}

type userFilter struct{}

func (f *userFilter) Accept(ctx context.Context, item user) (bool, error) {
	return item.BadgeNumber == 1, nil
}

type userMapper struct{}

func (m *userMapper) Map(ctx context.Context, item user) (string, error) {
	return item.Name, nil
}

func ExamplePipeline() {
	ctx := context.Background()

	var reader *sliceReader[user] = &sliceReader[user]{
		data: seedUsers(),
	}
	var filter *userFilter
	var mapper *userMapper
	var writer *stdWriter[string]

	if err := chainmorph.From(reader).
		Filter(filter).
		MapTo(mapper).
		WriteTo(ctx, writer); err != nil {
		fmt.Println("error:", err)
	}

	// Output:
	// Daniel
	// Zac
}

func seedUsers() []user {
	return []user{
		{
			Name:        "Daniel",
			Department:  "Software Support",
			BadgeNumber: 1,
		},
		{
			Name:        "Zac",
			Department:  "Apps Team",
			BadgeNumber: 1,
		},
		{
			Name:        "Andre",
			Department:  "Software Development",
			BadgeNumber: 2,
		},
		{
			Name:        "Jason",
			Department:  "Software Support",
			BadgeNumber: 2,
		},
	}
}
