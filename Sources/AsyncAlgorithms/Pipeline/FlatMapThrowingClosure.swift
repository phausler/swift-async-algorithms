extension Pipeline where Self: ~Copyable, Failure == Never {
  public consuming func flatMap<SegmentOfResult: Pipeline & ~Copyable, TransformedFailure: Error>(
    _ transform: nonisolated(nonsending) @escaping @Sendable (consuming Element) async throws(TransformedFailure) -> SegmentOfResult
  ) -> some Pipeline<SegmentOfResult.Element, TransformedFailure> & ~Copyable where SegmentOfResult.Failure == Never {
    return FlatMap(self, transform: transform)
  }
}

/*
A      | Base | SegmentOfResult | Closure |
-------------------------------------------
Throws |  ✅  |       ✅        |    ✅   |
-------------------------------------------
Never  |  ❌  |       ❌        |    ❌   |
-------------------------------------------

FlatMapNonThrowingClosure
B      | Base | SegmentOfResult | Closure |
-------------------------------------------
Throws |  ✅  |       ✅        |    ❌   |
-------------------------------------------
Never  |  ❌  |       ❌        |    ✅   |
-------------------------------------------

FlatMapThrowingBase
C      | Base | SegmentOfResult | Closure |
-------------------------------------------
Throws |  ✅  |       ❌        |    ❌   |
-------------------------------------------
Never  |  ❌  |       ✅        |    ✅   |
-------------------------------------------

FlatMapNonThrowingSegment
D      | Base | SegmentOfResult | Closure |
-------------------------------------------
Throws |  ✅  |       ❌        |    ✅   |
-------------------------------------------
Never  |  ❌  |       ✅        |    ❌   |
-------------------------------------------

FlatMapThrowingClosure
E      | Base | SegmentOfResult | Closure |
-------------------------------------------
Throws |  ❌  |       ❌        |    ✅   |
-------------------------------------------
Never  |  ✅  |       ✅        |    ❌   |
-------------------------------------------

FlatMapThrowingSegment
F      | Base | SegmentOfResult | Closure |
-------------------------------------------
Throws |  ❌  |       ✅        |    ❌   |
-------------------------------------------
Never  |  ✅  |       ❌        |    ✅   |
-------------------------------------------
*/

fileprivate struct FlatMap<Base: Pipeline & ~Copyable, SegmentOfResult: Pipeline & ~Copyable, TransformedFailure: Error>: ~Copyable where SegmentOfResult.Failure == Base.Failure, Base.Failure == Never {
  var base: Base
  var current: SegmentOfResult?
  var transform: (nonisolated(nonsending) @Sendable (consuming Base.Element) async throws(TransformedFailure) -> SegmentOfResult)?

  init(_ base: consuming Base, transform: nonisolated(nonsending) @escaping @Sendable (consuming Base.Element) async throws(TransformedFailure) -> SegmentOfResult) {
    self.base = base
    self.transform = transform
  }
}

extension FlatMap: Pipeline where Base: ~Copyable, SegmentOfResult: ~Copyable {
  typealias Element = SegmentOfResult.Element
  typealias Failure = TransformedFailure

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element? {
    while let transform {
      if current != nil {
        let optElement = await current!.request(isolation: isolation)
        guard let element = optElement else {
          current = nil
          continue
        }
        return element
      } else {
        do {
          let optItem = await base.request(isolation: isolation)
          guard let item = optItem else {
            self.transform = nil
            return nil
          }
          current = try await transform(item)
          let optElement = await current!.request(isolation: isolation)  
          guard let element = optElement else {
            current = nil
            continue
          }
          return element
        } catch {
          self.transform = nil
          throw error
        }
      }
    }
    return nil
  }
}