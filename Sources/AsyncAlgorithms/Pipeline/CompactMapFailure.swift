extension Pipeline  where Self: ~Copyable, Failure == Never {
  public consuming func compactMap<ElementOfResult: ~Copyable, TransfomredFailure: Error>(
    _ transform: nonisolated(nonsending) @escaping @Sendable (consuming Element) async throws(TransfomredFailure) -> ElementOfResult?
  ) -> some Pipeline<ElementOfResult, TransfomredFailure> & ~Copyable {
    CompactMap(self, transform: transform)
  }
}

fileprivate struct CompactMap<Base: Pipeline & ~Copyable, ElementOfResult: ~Copyable, TransfomredFailure: Error>: ~Copyable, Pipeline where Base.Failure == Never {
  typealias Element = ElementOfResult
  typealias Failure = TransfomredFailure

  var base: Base
  var transform: (nonisolated(nonsending) @Sendable (consuming Base.Element) async throws(TransfomredFailure) -> ElementOfResult?)?

  init(_ base: consuming Base, transform: nonisolated(nonsending) @escaping @Sendable (consuming Base.Element) async throws(TransfomredFailure) -> ElementOfResult?) {
    self.base = base
    self.transform = transform
  }

  mutating func request(isolation: isolated (any Actor)?) async throws(TransfomredFailure) -> Element? {
    while true {
      guard let transform else {
        return nil
      }
      do {
        guard let element = await base.request(isolation: isolation) else {
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
