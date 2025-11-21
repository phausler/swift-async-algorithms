extension Pipeline where Self: ~Copyable {
  public consuming func dropWhile(
    _ predicate: nonisolated(nonsending) @escaping @Sendable (borrowing Element) async throws(Failure) -> Bool
  ) -> some Pipeline<Element, Failure> & ~Copyable {
    DropWhile(self, predicate: predicate)
  }
}

fileprivate struct DropWhile<Base: Pipeline & ~Copyable>: ~Copyable {
  var base: Base
  var predicate: (nonisolated(nonsending) @Sendable (borrowing Element) async throws(Failure) -> Bool)?
  var finished = false

  init(_ base: consuming Base, predicate: nonisolated(nonsending) @escaping @Sendable (borrowing Element) async throws(Failure) -> Bool) {
    self.base = base
    self.predicate = predicate
  }
}

extension DropWhile: Pipeline where Base: ~Copyable {
  typealias Element = Base.Element
  typealias Failure = Base.Failure

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element? {
    guard !finished else {
      return nil
    }
    do {
      while let predicate = self.predicate {
        guard let element = try await base.request(isolation: isolation) else {
          self.predicate = nil
          self.finished = true
          return nil
        }
        if try await predicate(element) == false {
          self.predicate = nil
          return element
        }
      }
      return try await base.request(isolation: isolation)
    } catch {
      self.finished = true
      throw error
    }
  }
}