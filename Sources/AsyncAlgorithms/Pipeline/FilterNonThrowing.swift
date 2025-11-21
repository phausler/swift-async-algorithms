extension Pipeline where Self: ~Copyable {
  public consuming func filter(_ included: nonisolated(nonsending) @escaping @Sendable (borrowing Element) async -> Bool) -> some Pipeline<Element, Failure> & ~Copyable {
    Filter(self, included: included)
  }
}

fileprivate struct Filter<Base: Pipeline & ~Copyable>: ~Copyable {
  var base: Base
  var included: (nonisolated(nonsending) @Sendable (borrowing Base.Element) async -> Bool)?

  init(_ base: consuming Base, included: nonisolated(nonsending) @escaping @Sendable (borrowing Base.Element) async -> Bool) {
    self.base = base
    self.included = included
  }
}

extension Filter: Pipeline where Base: ~Copyable {
  typealias Element = Base.Element
  typealias Failure = Base.Failure

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element? {
    guard let included else {
      return nil
    }
    while true {
      do {
        guard let element = try await base.request(isolation: isolation) else { 
          self.included = nil
          return nil 
        }
        if await included(element) {
          return element
        }
      } catch {
        self.included = nil
        throw error
      }
    }
  }
}