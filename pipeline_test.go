package chainmorph

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
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

type spyTapper[T any] struct {
	counter int
	wantErr bool
}

func (t *spyTapper[T]) Tap(ctx context.Context, item T) error {
	if t.wantErr {
		return errors.New("tap error")
	}
	t.counter++
	return nil
}

func (t *spyTapper[T]) GetCount() int {
	return t.counter
}

func Test_From_SingleJSONelement(t *testing.T) {
	ctx := context.Background()

	type fileView struct {
		Element string `json:"element"`
		Max     int    `json:"max"`
		Min     int    `json:"min"`
	}

	jsonReader := &jsonReader[fileView]{
		path: "./testdata/example.json",
	}
	spyWriter := &spyWriter[fileView]{}

	err := From(jsonReader).
		WriteTo(ctx, spyWriter)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	want := fileView{
		Element: "PLC",
		Max:     10,
		Min:     2,
	}
	got := spyWriter.all()

	if len(got) != 1 {
		t.Fatalf("got %d items, want exactly 1", len(got))
	}

	if !reflect.DeepEqual(got[0], want) {
		t.Fatalf("got %v, want %v", got[0], want)
	}
}

func Test_From_ParserError(t *testing.T) {
	ctx := context.Background()

	type fileView struct {
		Element string `json:"element"`
		Max     int    `json:"max"`
		Min     int    `json:"min"`
	}

	jsonReader := &jsonReader[fileView]{
		path:    "./testdata/example.json",
		wantErr: true,
	}
	spyWriter := &spyWriter[fileView]{}

	err := From(jsonReader).
		WriteTo(ctx, spyWriter)
	if err == nil {
		t.Fatalf("expected error, got none")
	}
}

type jsonReader[T any] struct {
	path    string
	done    bool
	wantErr bool
}

func (f *jsonReader[T]) ReadFrom(ctx context.Context) (T, error) {
	var zero T
	if f.done {
		return zero, ErrEndOfStream
	}

	if f.wantErr {
		return zero, errors.New("Error!")
	}

	b, err := os.ReadFile(f.path)
	if err != nil {
		return zero, err
	}

	var res T
	err = json.Unmarshal(b, &res)
	if err != nil {
		return zero, err
	}

	f.done = true
	return res, nil
}

func Test_Just_ReadStream(t *testing.T) {
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

func Test_MapTo_ShouldMapToStrings(t *testing.T) {
	ctx := context.Background()
	source := []int{1, 2, 3, 4}
	spyWriter := &spyWriter[string]{}
	mapper := intToStringMapper{}

	err := Just(source...).
		MapTo(mapper).
		WriteTo(ctx, spyWriter)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	got := spyWriter.all()
	want := []string{"1", "2", "3", "4"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func Test_MapTo_ErrorWhileMapping(t *testing.T) {
	ctx := context.Background()
	source := []int{1, 2, 3, 4}
	spyWriter := &spyWriter[string]{}
	mapper := intToStringMapper{wantErr: true}

	err := Just(source...).
		MapTo(mapper).
		WriteTo(ctx, spyWriter)
	if err == nil {
		t.Fatalf("expecting error, got none")
	}
}

func Test_MapFunc_DoublesEachInt(t *testing.T) {
	ctx := context.Background()
	source := []int{1, 2, 3, 4}
	spyWriter := &spyWriter[int]{}

	err := Just(source...).
		MapFunc(func(ctx context.Context, i int) (int, error) {
			return i * 2, nil
		}).
		WriteTo(ctx, spyWriter)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	got := spyWriter.all()
	want := []int{2, 4, 6, 8}

	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func Test_MapFunc_ErrorWhileMapping(t *testing.T) {
	ctx := context.Background()
	source := []int{1, 2, 3, 4}
	spyWriter := &spyWriter[int]{}

	err := Just(source...).
		MapFunc(func(ctx context.Context, i int) (int, error) {
			return i, errors.New("error mapping")
		}).
		WriteTo(ctx, spyWriter)
	if err == nil {
		t.Fatalf("expecting error, got none")
	}
}

type intToStringMapper struct {
	wantErr bool
}

func (m intToStringMapper) Map(ctx context.Context, i int) (string, error) {
	if m.wantErr {
		return "", errors.New("got an error, skibidi")
	}
	return strconv.FormatInt(int64(i), 10), nil
}

func Test_If_SuccessfullyDropsItems(t *testing.T) {
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

func Test_FilterFunc_AppliesFuncSuccessfully(t *testing.T) {
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

func Test_FilterFunc_ExhaustsStreamWithFilters_ShouldNotError(t *testing.T) {
	source := []string{"A", "B", "C", "AB", "AC", "AD"}
	spyWriter := &spyWriter[string]{}
	ctx := context.Background()
	err := Just(source...).
		FilterFunc(func(ctx context.Context, s string) (bool, error) {
			return strings.HasPrefix(s, "Z"), nil
		}).
		WriteTo(ctx, spyWriter)

	want := []string{} // No elements
	got := spyWriter.all()

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !slices.Equal(want, got) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func Test_FilterFunc_ErrorAtFilterLevel(t *testing.T) {
	ctx := context.Background()
	source := []string{"A", "B", "C", "AB", "AC", "AD"}
	spyWriter := &spyWriter[string]{}
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

	if len(spyWriter.all()) > 0 {
		t.Fatalf("Not expecting elements, got %d", len(spyWriter.all()))
	}
}

func Test_Filter_UseFullFilterImplementation(t *testing.T) {
	ctx := context.Background()
	source := []string{"c", "A", "N", "I", "m"}
	spyWriter := &spyWriter[string]{}
	filter := &StringFilter{}
	err := Just(source...).
		Filter(filter).
		WriteTo(ctx, spyWriter)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"A"}
	got := spyWriter.all()

	if !slices.Equal(want, got) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

type StringFilter struct{}

func (f *StringFilter) Accept(ctx context.Context, s string) (bool, error) {
	return strings.Contains(s, "A"), nil
}

func Test_Tap_AllItemsAreCounted(t *testing.T) {
	ctx := context.Background()
	source := []string{"a", "b", "c", "d"}
	spyTapper := &spyTapper[string]{}
	spyWriter := &spyWriter[string]{}

	err := Just(source...).
		Tap(spyTapper).
		WriteTo(ctx, spyWriter)
	if err != nil {
		t.Fatalf("unexpected error %v", err)
	}

	want := len(source)
	got := spyTapper.GetCount()
	if spyTapper.GetCount() != len(source) {
		t.Fatalf("want %d, got %d", want, got)
	}
}

func Test_Tap_ErrorAtTapFunc(t *testing.T) {
	ctx := context.Background()
	source := []string{"a", "b", "c", "d"}
	spyTapper := &spyTapper[string]{wantErr: true}
	spyWriter := &spyWriter[string]{}

	err := Just(source...).
		Tap(spyTapper).
		WriteTo(ctx, spyWriter)
	if err == nil {
		t.Fatalf("expected error, got none")
	}
}
