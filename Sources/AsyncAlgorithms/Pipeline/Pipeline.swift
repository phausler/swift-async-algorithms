public protocol Pipeline<Element, Failure>: ~Copyable {
  associatedtype Element: ~Copyable
  associatedtype Failure: Error

  mutating func request(isolation: isolated (any Actor)?) async throws(Failure) -> Element?
}

extension Pipeline {
  public mutating func request(_ isolation: isolated (any Actor)? = #isolation) async throws(Failure) -> Element? {
    try await request(isolation: isolation)
  }
}