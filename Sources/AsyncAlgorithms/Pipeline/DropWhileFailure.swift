extension Pipeline where Self: ~Copyable, Failure == Never {
  public consuming func dropWhile<TransformedFailure: Error>(
    _ predicate: nonisolated(nonsending) @escaping @Sendable (borrowing Element) async throws(TransformedFailure) -> Bool
  ) -> some Pipeline<Element, TransformedFailure> & ~Copyable {
    DropWhile(self, predicate: predicate)
  }
}

fileprivate struct DropWhile<Base: Pipeline & ~Copyable, TransformedFailure: Error>: ~Copyable where Base.Failure == Never {
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
  typealias Failure = TransformedFailure

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element? {
    guard !finished else {
      return nil
    }
    do {
      while let predicate = self.predicate {
        guard let element = await base.request(isolation: isolation) else {
          self.predicate = nil
          self.finished = true
          return nil
        }
        if try await predicate(element) == false {
          self.predicate = nil
          return element
        }
      }
      return await base.request(isolation: isolation)
    } catch {
      self.finished = true
      throw error
    }
  }
}