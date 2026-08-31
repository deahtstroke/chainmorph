// Package chainmorph is a small ETL pipeline library built around Go 1.27's
// generic methods. It lets you compose readers, filters, processors, and
// writers into a single fluent, lazily-evaluated chain, including stages
// that change the item's type as it flows through.
//
// # Overview
//
// A [Pipeline] is a lazily-pulled stream. Nothing runs until a terminal
// method — [Pipeline.WriteTo] — is called. Every stage before that just
// builds up a chain of deferred work.
//
//	err := From(reader).
//		Filter(isEven).
//		MapTo(double).
//		WriteTo(ctx, writer)
//
// # Type-changing stages
//
// [Pipeline.MapTo] and [Pipeline.MapFunc] can change the pipeline's element
// type from T to R. This is only possible because Go 1.27 allows methods to
// declare their own type parameters, independent of the receiver's.
package chainmorph
