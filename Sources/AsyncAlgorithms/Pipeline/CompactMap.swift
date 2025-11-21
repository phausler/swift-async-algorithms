extension Pipeline where Self: ~Copyable {
  public consuming func compactMap<ElementOfResult: ~Copyable>(
    _ transform: nonisolated(nonsending) @escaping @Sendable (consuming Element) async throws(Failure) -> ElementOfResult?
  ) -> some Pipeline<ElementOfResult, Failure> & ~Copyable {
    CompactMap(self, transform: transform)
  }
}

fileprivate struct CompactMap<Base: Pipeline & ~Copyable, ElementOfResult: ~Copyable>: ~Copyable, Pipeline {
  typealias Element = ElementOfResult
  typealias Failure = Base.Failure

  var base: Base
  var transform: (nonisolated(nonsending) @Sendable (consuming Base.Element) async throws(Base.Failure) -> ElementOfResult?)?

  init(_ base: consuming Base, transform: nonisolated(nonsending) @escaping @Sendable (consuming Base.Element) async throws(Base.Failure) -> ElementOfResult?) {
    self.base = base
    self.transform = transform
  }

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element? {
    while true {
      guard let transform else {
        return nil
      }
      do {
        guard let element = try await base.request(isolation: isolation) else {
          self.transform = nil
          return nil
        }
        if let transformed = try await transform(element) {
          return transformed
        }
      } catch {
        self.transform = nil
        throw error
      }
    }
  }
}
