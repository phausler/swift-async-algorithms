extension Pipeline where Failure == Never {
  public consuming func map<Transformed: ~Copyable, TransformedFailure: Error>(
    _ transform: nonisolated(nonsending) @escaping @Sendable (consuming Element) async throws(TransformedFailure) -> Transformed
  ) -> some Pipeline<Transformed, TransformedFailure> & ~Copyable {
    Map(self, transform: transform)
  }
}

fileprivate struct Map<Base: Pipeline, Transformed: ~Copyable, TransformedFailure: Error>: ~Copyable, Pipeline where Base.Failure == Never {
  typealias Element = Transformed
  typealias Failure = TransformedFailure

  var base: Base

  var transform: (nonisolated(nonsending) @Sendable (consuming Base.Element) async throws(TransformedFailure) -> Transformed)?

  init(_ base: Base, transform: nonisolated(nonsending) @escaping @Sendable (consuming Base.Element) async throws(TransformedFailure) -> Transformed) {
    self.base = base
    self.transform = transform
  }

  mutating func request(isolation: isolated (any Actor)?) async throws(TransformedFailure) -> Transformed? {
    guard let transform else {
      return nil
    }
    do {
      guard let element = await base.request(isolation: isolation) else { 
        self.transform = nil
        return nil 
      }
      return try await transform(element)
    } catch {
      self.transform = nil
      throw error
    }
  }
}