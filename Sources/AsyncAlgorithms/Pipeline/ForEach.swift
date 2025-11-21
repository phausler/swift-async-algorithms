extension Pipeline {
  public consuming func forEach(isolation: isolated (any Actor)? = #isolation, _ apply: nonisolated(nonsending) (consuming Element) async throws(Failure) -> Void) async throws(Failure) {
    while let element = try await request(isolation: isolation) {
      try await apply(element)
    }
  }
}