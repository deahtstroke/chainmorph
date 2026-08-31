package chainmorph

import (
	"context"
	"errors"
)

// ErrEndOfStream signals that an [ItemReader] has no more items to produce.
// Implementations of [ItemReader.ReadFrom] must return this error wrapped or unwrapped.
// Once exhausted, Pipeline has no other way to detect the end of a stream.
// Returning any other error is treated as a failure and stops the pipeline,
// returning nil forever will cause the pipeline to pull indefinitely.
var ErrEndOfStream = errors.New("end of stream")

// Pipeline represents a chain of lazily-evaluated steps applied to a stream
// of items of type T. No item is read or transformed until a terminal operation
// such as [Pipeline.WriteTo] is called.
type Pipeline[T any] struct {
	pull func(context.Context) (T, bool, error)
}

// ItemReader produces a stream of items of type T, one per call to ReadFrom.
// Implementations must return [ErrEndOfStream] once exhausted.
type ItemReader[T any] interface {
	ReadFrom(context.Context) (T, error)
}

// Tapper peforms a side effect on each item passing through the Pipeline such as
// logging, metrics, publishing events, etc. without altering the item itself
// See [Pipeline.Tap].
type Tapper[T any] interface {
	Tap(context.Context, T) error
}

// ItemWriter is a terminal sink that consumes a single item of type T,
// such as writing it to a database, file, or external service.
type ItemWriter[T any] interface {
	Write(context.Context, T) error
}

// ItemFilter decides whether an item should continue through the pipeline.
// See [Pipeline.Filter].
type ItemFilter[T any] interface {
	Accept(context.Context, T) (bool, error)
}

// ItemMapper transforms an item of type T into an item of type R. See [Pipeline.MapTo],
// which is the methods that lets a pipeline's element type change mid-chain.
type ItemMapper[T any, R any] interface {
	Map(context.Context, T) (R, error)
}

// Predicate adapts a plain function into an [ItemFilter].
// It's the mechanism behind [Pipeline.FilterFunc] and [Pipeline.If].
type Predicate[T any] func(context.Context, T) (bool, error)

func (p Predicate[T]) Accept(ctx context.Context, item T) (bool, error) {
	return p(ctx, item)
}

// Just builds a pipeline that yields each of the elements in order, then ends.
// Unlike [From], it needs no ItemReader: elements are consumed directly.
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

// From builds a pipeline sourced from itemReader. See [ErrEndOfStream] for
// the contract itemReader must follow to signal completion.
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

// Filter keeps only items for which itemFilter.Accept returns true
// Rejected items are silently skipped. They are never surfaced to callers
// or to later pipeline stages.
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

// FilterFunc wraps f as a [Predicate] and calls Filter with it, letting callers
// pass a plain function instead of an [ItemFilter] implementation.
func (p *Pipeline[T]) FilterFunc(f func(context.Context, T) (bool, error)) *Pipeline[T] {
	return p.Filter(Predicate[T](f))
}

// If is a simplified [Pipeline.Filter] for predicates that don't need context or
// the ability to fail
func (p *Pipeline[T]) If(f func(item T) bool) *Pipeline[T] {
	return p.Filter(Predicate[T](func(ctx context.Context, t T) (bool, error) {
		return f(t), nil
	}))
}

// WriteTo drains the pipeline, pulling and writing items one at a time
// until the source is exhausted or an error occurs. It returns nil once
// the pipeline ends cleanly, or the first error encountered, either from
// upstream or itemWriter.Write
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

// MapperFunc adapts a plain function in an [ItemMapper]. It's the mechanism
// behind [Pipeline.MapFunc]
type MapperFunc[T any, R any] func(context.Context, T) (R, error)

func (m MapperFunc[T, R]) Map(ctx context.Context, item T) (R, error) {
	return m(ctx, item)
}

// MapTo transforms each item from T to R using itemMapper, changing the pipeline's
// element type. This method's own type parameter, R, is independent of the receiver's T.
// This capability is only available to methods since Go 1.27.
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

// MapFunc wraos f as a [MapperFunc] and calls MapTo with it, letting callers
// pass a plain function instead of an [ItemMapper] implementation.
func (p *Pipeline[T]) MapFunc[R any](f func(context.Context, T) (R, error)) *Pipeline[R] {
	return p.MapTo(MapperFunc[T, R](f))
}

// Tap runs tapper.Tap on each item as a side effect, then passes the item through unchanged.
// Use it for logging, metrics, or publishing events without altering the pipeline's data.
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

// TapFunc adapts a plain function into a [Tapper]. It's the mechanism behind [Pipeline.TapFunc]
type TapFunc[T any] func(context.Context, T) error

func (t TapFunc[T]) Tap(ctx context.Context, item T) error {
	return t(ctx, item)
}

// TapFunc wraps f as a [TapFunc] and calls Tap with it, letting callers pass a
// plain function instead of a [Tapper] implementation.
func (p *Pipeline[T]) TapFunc(f func(context.Context, T) error) *Pipeline[T] {
	return p.Tap(TapFunc[T](f))
}
