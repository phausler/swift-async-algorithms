//===----------------------------------------------------------------------===//
//
// This source file is part of the Swift Async Algorithms open source project
//
// Copyright (c) 2022 Apple Inc. and the Swift project authors
// Licensed under Apache License v2.0 with Runtime Library Exception
//
// See https://swift.org/LICENSE.txt for license information
//
//===----------------------------------------------------------------------===//

extension AsyncSequence where Self: Sendable, AsyncIterator: Sendable, Element: Sendable {
  public func share() -> AsyncShareSequence<Self> {
    AsyncShareSequence(self)
  }
}

public struct AsyncShareSequence<Base: AsyncSequence>: AsyncSequence
  where Base: Sendable, Base.AsyncIterator: Sendable, Base.Element: Sendable {
   public typealias Element = Base.Element
   struct Split: Sendable {
     enum Resumer {
       case primary // the actual driving iterator
       case dependent // dependent upon a primary iterator to produce a value
       case cancelled // no longer present for this id
     }
     
     // The state for a specific iteration "side" of the share
     enum Side: Sendable {
       case idle
       case waiting(UnsafeContinuation<Resumer, Never>) // someone is awaiting the iteration to start
       case placeholder // someone has the continuation and performing either a primary or dependent iteration
       case resolved(Result<Base.Element?, Error>) // some value has been resolved already
       case pending(UnsafeContinuation<Result<Base.Element?, Error>, Never>) // waiting for a pending value to be resolved
     }
     
     // the state for iteration of the base
     enum Upstream: Sendable {
       case idle(Base)
       case active(Base.AsyncIterator)
     }
     
     // The shared state of all sides that are being iterated.
     struct State: Sendable {
       var sides = [Int: Side]()
       var upstream: Upstream
       
       init(_ base: Base) {
         upstream = .idle(base)
       }
       
       // gather up all waiting sides so that they are then resumed accordingly to the picked primary iterator
       mutating func enter(_ side: Int) -> [UnsafeContinuation<Resumer, Never>] {
         var indices = [Int]()
         var collected = [UnsafeContinuation<Resumer, Never>]()
         for index in sides.keys {
           switch sides[index] {
           case .waiting(let continuation):
             indices.append(index)
             if side != index {
               collected.append(continuation)
             }
           default:
             return [] // not fully ready yet, someone isnt yet to the waiting phase
           }
         }
         for index in indices {
           // set the states to the placeholder so that the continuation wont be potentially picked up more than once
           sides[index] = .placeholder
         }
         return collected
       }
       
       // create or get the iterator from the base for primary iteration
       mutating func makeAsyncIterator() -> Base.AsyncIterator {
         switch upstream {
         case .idle(let base):
           return base.makeAsyncIterator()
         case .active(let iterator):
           return iterator
         }
       }
       
       // write back the base's iterator as an active state
       mutating func setIterator(_ iterator: Base.AsyncIterator) {
         upstream = .active(iterator)
       }
       
       @discardableResult
       static func cancel(_ state: ManagedCriticalState<State>, side: Int) -> Bool {
         var forward = false
         var waiting: UnsafeContinuation<Resumer, Never>?
         var pending: UnsafeContinuation<Result<Base.Element?, Error>, Never>?
         let continuations = state.withCriticalRegion { state -> [UnsafeContinuation<Resumer, Never>] in
           switch state.sides[side] {
           case .waiting(let continuation):
             waiting = continuation
           case .pending(let continuation):
             pending = continuation
           default: break
           }
           state.sides.removeValue(forKey: side)
           
           for index in state.sides.keys {
             if index != side {
                return state.enter(side)
             }
           }
           forward = true
           
           return state.enter(side)
         }
         
         waiting?.resume(returning: .dependent)
         var continuationIterator = continuations.makeIterator()
         if let first = continuationIterator.next() {
           while let continuation = continuationIterator.next() {
             continuation.resume(returning: .dependent)
           }
           first.resume(returning: .primary)
         }
         pending?.resume(returning: .success(nil))
         return forward
       }
       
       static func resumer(_ state: ManagedCriticalState<State>, side: Int) async -> Resumer {
         await withUnsafeContinuation { continuation in
           guard let (continuations, single) = state.withCriticalRegion({ state -> ([UnsafeContinuation<Resumer, Never>], Bool)? in
             guard state.sides[side] != nil else {
               // if the side is no long present then we know it has been cancelled
               return nil
             }
             if state.sides.count == 1 {
               return ([], true)
             }
             state.sides[side] = .waiting(continuation)
             return (state.enter(side), false)
           }) else {
             // resume as cancelled since no side was found for the index
             continuation.resume(returning: .cancelled)
             return
           }
           if single {
             continuation.resume(returning: .primary)
           } else {
             for continuation in continuations {
               continuation.resume(returning: .dependent)
             }
             if continuations.count > 0 {
               continuation.resume(returning:  .primary)
             }
           }
         }
       }
     }
     
     let state: ManagedCriticalState<State>
     
     init(_ base: Base) {
       state = ManagedCriticalState(State(base))
     }
     
     func makeSide() -> Int {
       return state.withCriticalRegion { state -> Int in
         let index = state.sides.count
         state.sides[index] = .idle
         return index
       }
     }
     
     func cancel(_ side: Int) {
       State.cancel(state, side: side)
     }
     
     func next(_ side: Int) async -> Result<Base.Element?, Error> {
       let resumer = await State.resumer(state, side: side)
       switch resumer {
       case .primary:
         // this should be refactored to send events into a running task instead of creating a task for each primary iteration
         let task: Task<Result<Base.Element?, Error>, Never> = Task {
           var iterator = state.withCriticalRegion { $0.makeAsyncIterator() }
           let result: Result<Base.Element?, Error>
           do {
             let value = try await iterator.next()
             result = .success(value)
           } catch {
             result = .failure(error)
           }
           
           var continuations = [UnsafeContinuation<Result<Base.Element?, Error>, Never>]()
           state.withCriticalRegion { state in
             for index in state.sides.keys {
               switch state.sides[index] {
               case .placeholder:
                 state.sides[index] = .resolved(result)
               case .pending(let continuation):
                 state.sides[index] = .idle
                 continuations.append(continuation)
               default:
                 break
               }
             }
             state.setIterator(iterator)
           }
           for continuation in continuations {
             continuation.resume(returning: result)
           }
           return result
         }
         return await withTaskCancellationHandler {
           let forward = State.cancel(state, side: side)
           if forward {
             task.cancel()
           }
         } operation: {
           return await task.value
         }
       case .dependent:
         return await withUnsafeContinuation { continuation in
           let result = state.withCriticalRegion { state -> Result<Base.Element?, Error>? in
             switch state.sides[side] {
             case .resolved(let result):
               state.sides[side] = .idle
               return result
             default:
               state.sides[side] = .pending(continuation)
               return nil
             }
           }
           if let result = result {
             continuation.resume(returning: result)
           }
         }
       case .cancelled:
         return .success(nil)
       }
     }
   }

   public struct Iterator: AsyncIteratorProtocol {
     final class Side: Sendable {
       let split: Split
       let side: Int
       
       init(_ split: Split, side: Int) {
         self.split = split
         self.side = side
       }
       
       deinit {
         split.cancel(side)
       }

       func next() async rethrows -> Base.Element? {
         return try await split.next(side)._rethrowGet()
       }
     }
     
     let side: Side
     
     init(_ split: Split, side: Int) {
       self.side = Side(split, side: side)
     }
     
     public mutating func next() async rethrows -> Base.Element? {
       return try await side.next()
     }
   }
   
   let split: Split
   
   init(_ base: Base) {
     self.split = Split(base)
   }

   public func makeAsyncIterator() -> Iterator {
     Iterator(split, side: split.makeSide())
   }
 }

 extension AsyncShareSequence: Sendable where Base.Element: Sendable { }
 extension AsyncShareSequence.Iterator: Sendable where Base.Element: Sendable { }
