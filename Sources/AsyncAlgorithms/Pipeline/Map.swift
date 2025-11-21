extension Pipeline {
  public consuming func map<Transformed: ~Copyable>(
    _ transform: nonisolated(nonsending) @escaping @Sendable (consuming Element) async throws(Failure) -> Transformed
  ) -> some Pipeline<Transformed, Failure> & ~Copyable {
    Map(self, transform: transform)
  }
}

fileprivate struct Map<Base: Pipeline, Transformed: ~Copyable>: ~Copyable, Pipeline {
  typealias Element = Transformed
  typealias Failure = Base.Failure

  var base: Base

  var transform: (nonisolated(nonsending) @Sendable (consuming Base.Element) async throws(Base.Failure) -> Transformed)?

  init(_ base: Base, transform: nonisolated(nonsending) @escaping @Sendable (consuming Base.Element) async throws(Base.Failure) -> Transformed) {
    self.base = base
    self.transform = transform
  }

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element? {
    guard let transform else {
      return nil
    }
    do {
      guard let element = try await base.request(isolation: isolation) else { 
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