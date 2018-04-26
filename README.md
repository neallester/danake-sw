# Danake-SW

The Danake framework provides Swift application developers with tools to move data between an application and external persistent storage with thread safe and lightweight application data structures.

## Introduction

Danake provides a relatively simple and straightforward framework for saving and retrieving small portions of an application model which features complex instance graphs. Model constructs only need to implement the *Codable* protocol, but no attempt is made to separate the persistence system **machine** code from the application **model** code. The philosophy is: You are writing a machine, not a model; get over it. Communication with persistent media is handled through an adapter allowing media agnostic application code, and a mock persistent media class [InMemoryAccessor](https://github.com/neallester/danake-sw/blob/master/Sources/danake/InMemoryAccessor.swift) is provided for testing application code. A Mongo DB adapter is planned soon. Problems with the persistent media are (in general) reported using status enums rather than exceptions.

Application developers work with a wrapper ([Entity<T: Codable>](https://github.com/neallester/danake-sw/blob/master/Sources/danake/entity.swift)) around their model constructs (T is the type of the model instance). Each Entity object is stored, retrieved, and cached independently. The Entity wrapper also provides thread safe asynchronous and synchronous access to the model instance. References to Entities are implemented using the [EntityReference<T: Codable>](https://github.com/neallester/danake-sw/blob/master/Sources/danake/EntityReference.swift) class. Lazy (asynchronous and synchronous) and eager retrieval are supported.

Data updates are collected into batches which are submitted manually by the application developer. Batch processing is asynchronous. Persistent media error reporting and framework managed retry (for the life of the process) are provided (unrecoverable errors are reported but not retried). Database writes are eventually consistent.

Please see the [SampleUsageTests](https://github.com/neallester/danake-sw/blob/master/Tests/danakeTests/SampleUsageTests.swift) for more detail about incorporating the Danake framework into application code.

## Long Term Aspirations
* Mismatch correction for persistent data structure changes
* Compile time identification of persistent data structure changes which may be incompatible with existing stored data
* Batch consistency for database writes
* Full persistence (entity versioning)
* Consistent database view within a batch
* Additional adapters
* Support for read only access
* Database errors written to secondary media with post restart recovery managed by the framework
* Query abstractions
* Inter process entity locking,collision resolution and/or field level merge.
* Reference counting
* Intra and inter process pub/sub
* Hooks for adding undo/redo support to applications
* Add support for re-creating Database and/or PersistentCollection objects.


