extension Pipeline {
  public consuming func flatMap<SegmentOfResult: Pipeline>(
    _ transform: nonisolated(nonsending) @escaping @Sendable (consuming Element) async throws(Failure) -> SegmentOfResult
  ) -> some Pipeline<SegmentOfResult.Element, Failure> & ~Copyable where SegmentOfResult.Failure == Failure {
    return FlatMap(self, transform: transform)
  }
}

fileprivate struct FlatMap<Base: Pipeline, SegmentOfResult: Pipeline> where SegmentOfResult.Failure == Base.Failure {
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
  typealias Failure = Base.Failure

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element? {
    while let transform {
      if var segment = current {
        do {
          let optElement = try await segment.request(isolation: isolation)
          guard let element = optElement else {
            current = nil
            continue
          }
          // restore the segment since we just mutated it with request
          current = segment
          return element
        } catch {
          self.transform = nil
          throw error
        }
      } else {
        do {
          let optItem = try await base.request(isolation: isolation)
          guard let item = optItem else {
            self.transform = nil
            return nil
          }
          var segment = try await transform(item)
          let optElement = try await segment.request(isolation: isolation)  
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