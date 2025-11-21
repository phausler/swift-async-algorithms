extension Pipeline where Self: ~Copyable {
  public consuming func dropFirst(
    _ count: Int = 1
  ) -> some Pipeline<Element, Failure> & ~Copyable {
    DropFirst(self, count: count)
  }
}

fileprivate struct DropFirst<Base: Pipeline & ~Copyable>: ~Copyable, Pipeline {
  typealias Element = Base.Element
  typealias Failure = Base.Failure

  var base: Base
  var count: Int

  init(_ base: consuming Base, count: Int) {
    self.base = base
    self.count = count
  }

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element? {
    var remainingToDrop = count
    while remainingToDrop > 0 {
      guard try await base.request(isolation: isolation) != nil else {
        count = 0
        return nil
      }
      remainingToDrop -= 1
    }
    count = 0
    return try await base.request(isolation: isolation)
  }
}