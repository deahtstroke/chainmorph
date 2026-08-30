package chainmorph

import (
	"context"
	"errors"
)

var ErrEndOfStream = errors.New("end of stream")

// Pipeline represents a series of sequentially chaining steps that are taken
// to do ETL operations on some item(s)
//
// They are lazily evaluated until a terminator operation
type Pipeline[T any] struct {

	// pull represents the lazily evaluated function thats currently on the
	// chain of events, every operations besides initalizer and terminator
	// operations acts as closures ont the previous step in the Pipeline
	pull func(context.Context) (T, bool, error)
}

// An ItemReader represents any step that is able to read an arbitrary item of type T
// and chain it downstream to the proceeding operations
type ItemReader[T any] interface {
	ReadFrom(context.Context) (T, error)
}

// A Tapper represents any side-effect after a step in the pipeline such as logging,
// publishing events, etc.
type Tapper[T any] interface {
	Tap(context.Context, T) error
}

// An ItemWriter is a terminator operation involves writing the transformed data to
// a data sink
type ItemWriter[T any] interface {
	Write(context.Context, T) error
}

// An ItemFilter will filter out the upstream item(s) based on some criterion
type ItemFilter[T any] interface {
	Accept(context.Context, T) (bool, error)
}

// ItemMapper takes in an element of type T and maps it to an element of type R
type ItemMapper[T any, R any] interface {
	Map(context.Context, T) (R, error)
}

// AfterAction defines a no-op action to do after a step in the pipeline
type AfterAction[T any] interface {
	DoAfter(context.Context, T) error
}

// The Predicate type wraps a function that evaluates an item of type T
// and returns true/false or a fatal error when something occurs
type Predicate[T any] func(context.Context, T) (bool, error)

func (p Predicate[T]) Accept(ctx context.Context, item T) (bool, error) {
	return p(ctx, item)
}

// Just takes in a slice of elements of type T and returns each element
// Like the method name, is 'just' a stream of elements from a slice
func Just[T any](elems ...T) *Pipeline[T] {
	return &Pipeline[T]{
		pull: func(ctx context.Context) (T, bool, error) {
			var zero T
			if len(elems) <= 0 {
				return zero, false, nil
			}

			var elem T
			elem, elems = elems[0], elems[1:]
			return elem, true, nil
		},
	}
}

// TODO: There's no real way to know if an ItemReader really finished reading
// unless they explicitly pass the ErrEndOfStream error, this constraints
// implementations into using this to not let pipelines run infinitely
func From[T any](itemReader ItemReader[T]) *Pipeline[T] {
	return &Pipeline[T]{
		pull: func(ctx context.Context) (T, bool, error) {
			var zero T
			result, err := itemReader.ReadFrom(ctx)
			if errors.Is(err, ErrEndOfStream) {
				return zero, false, nil
			}

			if err != nil {
				return zero, false, err
			}

			return result, true, nil
		},
	}
}

// Filter takes in an implementation of itemFilter and applies
// the criteria to the result of the previous step
func (p *Pipeline[T]) Filter(itemFilter ItemFilter[T]) *Pipeline[T] {
	prev := p.pull
	return &Pipeline[T]{
		pull: func(ctx context.Context) (T, bool, error) {
			var zero T
			for {
				item, ok, err := prev(ctx)
				if !ok || err != nil {
					return zero, false, err
				}

				accepted, err := itemFilter.Accept(ctx, item)
				if err != nil {
					return zero, false, err
				}

				if accepted {
					return item, accepted, nil
				}
			}
		},
	}
}

// This step lets you define a custom function that satisfies the
// type definition of a Preidcate, wraps it inside a Predicate, and
// calls Filter itself
func (p *Pipeline[T]) FilterFunc(f func(context.Context, T) (bool, error)) *Pipeline[T] {
	return p.Filter(Predicate[T](f))
}

// If is an even-simpler step that does not care about context or
// errors, it receives a function that returns true/false and filters based
// on that criteria
func (p *Pipeline[T]) If(f func(item T) bool) *Pipeline[T] {
	return p.Filter(Predicate[T](func(ctx context.Context, t T) (bool, error) {
		return f(t), nil
	}))
}

// WriteTo is a terminator step that pulls a single item from upstream and writes
// it to the passed in data sync itemWriter
func (p *Pipeline[T]) WriteTo(ctx context.Context, itemWriter ItemWriter[T]) error {
	for {
		res, ok, err := p.pull(ctx)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		if err := itemWriter.Write(ctx, res); err != nil {
			return err
		}
	}
}

type MapperFunc[T any, R any] func(context.Context, T) (R, error)

func (m MapperFunc[T, R]) Map(ctx context.Context, item T) (R, error) {
	return m(ctx, item)
}

// MapTo lets you map an item of type T to an alternate item of type R
func (p *Pipeline[T]) MapTo[R any](itemMapper ItemMapper[T, R]) *Pipeline[R] {
	prev := p.pull
	return &Pipeline[R]{
		pull: func(ctx context.Context) (R, bool, error) {
			var zero R
			item, ok, err := prev(ctx)
			if !ok || err != nil {
				return zero, false, err
			}

			new, err := itemMapper.Map(ctx, item)
			if err != nil {
				return zero, false, err
			}

			return new, true, nil
		},
	}
}

// MapFunc is a step that lets you inline a mapping function on the pipeline itself
func (p *Pipeline[T]) MapFunc[R any](f func(context.Context, T) (R, error)) *Pipeline[R] {
	return p.MapTo(MapperFunc[T, R](f))
}

// Tap takes in an implementation of Tapper and applies the Tap function
func (p *Pipeline[T]) Tap(tapper Tapper[T]) *Pipeline[T] {
	prev := p.pull
	return &Pipeline[T]{
		pull: func(ctx context.Context) (T, bool, error) {
			item, ok, err := prev(ctx)
			if !ok || err != nil {
				return item, ok, err
			}

			if err := tapper.Tap(ctx, item); err != nil {
				var zero T
				return zero, false, err
			}

			return item, true, nil
		},
	}
}

type TapFunc[T any] func(context.Context, T) error

func (t TapFunc[T]) Tap(ctx context.Context, item T) error {
	return t(ctx, item)
}

func (p *Pipeline[T]) TapFunc(f func(context.Context, T) error) *Pipeline[T]{
	return p.Tap(TapFunc[T](f))
}
