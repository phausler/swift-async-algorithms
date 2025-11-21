extension Pipeline where Self: ~Copyable, Failure == Never {
  public consuming func filter<TransfomredFailure: Error>(_ included: nonisolated(nonsending) @escaping @Sendable (borrowing Element) async throws(TransfomredFailure) -> Bool) -> some Pipeline<Element, TransfomredFailure> & ~Copyable {
    Filter(self, included: included)
  }
}

fileprivate struct Filter<Base: Pipeline & ~Copyable, TransfomredFailure: Error>: ~Copyable where Base.Failure == Never {
  var base: Base
  var included: (nonisolated(nonsending) @Sendable (borrowing Base.Element) async throws(TransfomredFailure) -> Bool)?

  init(_ base: consuming Base, included: nonisolated(nonsending) @escaping @Sendable (borrowing Base.Element) async throws(TransfomredFailure) -> Bool) {
    self.base = base
    self.included = included
  }
}

extension Filter: Pipeline where Base: ~Copyable {
  typealias Element = Base.Element
  typealias Failure = TransfomredFailure

  mutating func request(isolation: isolated (any Actor)?) async throws(TransfomredFailure) -> Element? {
    guard let included else {
      return nil
    }
    while true {
      do {
        guard let element = await base.request(isolation: isolation) else { 
          self.included = nil
          return nil 
        }
        if try await included(element) {
          return element
        }
      } catch {
        self.included = nil
        throw error
      }
    }
  }
}