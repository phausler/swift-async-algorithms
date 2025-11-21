extension Pipeline where Failure == Never{
  public consuming func flatMap<SegmentOfResult: Pipeline, TransformedFailure: Error>(
    _ transform: nonisolated(nonsending) @escaping @Sendable (consuming Element) async throws(TransformedFailure) -> SegmentOfResult
  ) -> some Pipeline<SegmentOfResult.Element, TransformedFailure> & ~Copyable where SegmentOfResult.Failure == Never {
    return FlatMap(self, transform: transform)
  }
}

fileprivate struct FlatMap<Base: Pipeline, SegmentOfResult: Pipeline, TransformedFailure: Error> where SegmentOfResult.Failure == Base.Failure, Base.Failure == Never {
  var base: Base
  var current: SegmentOfResult?
  var transform: (nonisolated(nonsending) @Sendable (consuming Base.Element) async throws(Failure) -> SegmentOfResult)?

  init(_ base: Base, transform: nonisolated(nonsending) @escaping @Sendable (consuming Base.Element) async throws(Failure) -> SegmentOfResult) {
    self.base = base
    self.transform = transform
  }
}

extension FlatMap: Pipeline {
  typealias Element = SegmentOfResult.Element
  typealias Failure = TransformedFailure

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element? {
    while let transform {
      if var segment = current {
        let optElement = await segment.request(isolation: isolation)
        guard let element = optElement else {
          current = nil
          continue
        }
        // restore the segment since we just mutated it with request
        current = segment
        return element
      } else {
        let optItem = await base.request(isolation: isolation)
        guard let item = optItem else {
          self.transform = nil
          return nil
        }
        do {
          var segment = try await transform(item)
          let optElement = await segment.request(isolation: isolation)  
          guard let element = optElement else {
            current = nil
            continue
          }
          current = segment
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