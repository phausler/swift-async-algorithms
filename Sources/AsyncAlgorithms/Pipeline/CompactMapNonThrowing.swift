extension Pipeline {
  public consuming func compactMap<ElementOfResult: ~Copyable>(
    _ transform: nonisolated(nonsending) @escaping @Sendable (consuming Element) async -> ElementOfResult?
  ) -> some Pipeline<ElementOfResult, Failure> & ~Copyable {
    CompactMap(self, transform: transform)
  }
}

fileprivate struct CompactMap<Base: Pipeline, ElementOfResult: ~Copyable>: ~Copyable, Pipeline {
  typealias Element = ElementOfResult
  typealias Failure = Base.Failure

  var base: Base
  var transform: (nonisolated(nonsending) @Sendable (consuming Base.Element) async -> ElementOfResult?)?

  init(_ base: Base, transform: nonisolated(nonsending) @escaping @Sendable (consuming Base.Element) async -> ElementOfResult?) {
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
        if let transformed = await transform(element) {
          return transformed
        }
      } catch {
        self.transform = nil 
        throw error
      }
    }
  }
}
