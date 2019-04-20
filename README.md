# Danake-SW

The Danake-Swift framework manages the movement of data between an application process and external persistent storage with lightweight application data structures.

## Introduction

Danake is a relatively simple framework for saving and retrieving small portions of an application model which features complex instance graphs. Model constructs only need to implement the *Codable* protocol, but no attempt is made to separate the persistence system **machine** code from the application **model** code. The philosophy is: You are writing a machine, not a model; get over it. Communication with persistent media is handled through an adapter allowing media agnostic application code, and a mock persistent media class [InMemoryAccessor](https://github.com/neallester/danake-sw/blob/master/Sources/danake/InMemoryAccessor.swift) is provided for testing application code. A [Mongo DB adapter](https://github.com/neallester/danake-mongo) is available. The framework is currently most suitable for applications where only one process will update the persistent media or where an upstream process can reliably route requests to processes which each handle independent subsets of the object graph.

Application developers work with a wrapper ([Entity<T: Codable>](https://github.com/neallester/danake-sw/blob/master/Sources/danake/entity.swift)) around their model constructs (T is the type of the model instance). Each Entity object is stored, retrieved, and cached independently. The framework ensures that each persistent object is represented by one and only one instance per process. The Entity wrapper also provides thread safe asynchronous and synchronous access to the model instance. (something like an incompletely implemented [Actor](https://gist.github.com/lattner/31ed37682ef1576b16bca1432ea9f782#part-2-actors-eliminating-shared-mutable-state)). References to Entities are implemented using the [ReferenceManager<P: Codable, T: Codable>](https://github.com/neallester/danake-sw/blob/master/Sources/danake/ReferenceManager.swift) class (P=PARENT, T=TYPE). Lazy (asynchronous and synchronous) and eager retrieval are supported.

Data updates are collected into batches which are submitted manually by the application developer. Batch processing is asynchronous. Persistent media error reporting and framework managed retry (for the life of the process) are provided (unrecoverable errors are reported but not retried). Database writes are eventually consistent.

The asynchronous API is based on [PromiseKit](https://github.com/mxcl/PromiseKit) and developers should consult that website for more information about working with promises. 

Please see the [SampleUsage](https://github.com/neallester/danake-sw/blob/master/Sources/danake/SampleUsage.swift) for more detail about incorporating the Danake framework into application code.

## Status

This is library is currently tested on OSX High Sierra and Ubuntu 18.04 with Swift 5.0

## Installation

Install the [danake-mongo](https://github.com/neallester/danake-mongo) Accessor which includes this framework.

## Gotchas

* Removing Entities (Entity.remove(batch:)) which are referenced by other Entities from persistent storage will generate errors when the remaining references are used. 
* Application developers are responsible for removing Entities ((Entity.remove(batch:)) which are not needed as persistent media entry points and which are not referenced by any other Entities from persistent media.
* Unless EntityReference is used, attributes which reference objects (that is, attributes which are implemented as classes rather than structs) will be stored and retrieved with struct (value) semantics rather than class (reference) semantics.
* Changes to Entity items must be done via Entity.update(), not Entity.sync(), but the compiler will only enforce this restriction for closures which directly assign to Struct attributes. Ensure that any changes to classes or which occur as side effects to function calls occur within an Entity.update() closure.
* Assigning a reference to an Entity item outside the closure used to access it defeats thread safety and is not detected by any compile or run type checks.

## Setup Development Environment on OSX
1. Install [sourcery](https://github.com/krzysztofzablocki/Sourcery)
1. Create a file named sourcery.sh in the project root directory (the directory which contains README.md). This file name is entered in .gitignore so it will not be checked in. This file should contain:

   /path/to/binary/sourcery --sources Tests/ --templates sourcery/LinuxMain.stencil --args testimports='@testable import danakeTests' --output Tests
1. chmod +x sourcery.sh

## Short Term Goals

* [MongoKitten Accessor](https://github.com/OpenKitten/MongoKitten)
* [Foundation DB Accessor](https://github.com/kirilltitov/FDBSwift)
* Batch Consistency (on media supporting it)

## Long Term Aspirations
* Mismatch correction for persistent data structures which are incompatible with current application code
* Compile time identification of persistent data structure changes which may be incompatible with existing stored data
* Full persistence (entity versioning)
* Consistent database view within a batch
* Additional adapters
* Support for read only access
* Database errors written to secondary media with post restart recovery managed by the framework
* Query abstractions
* Inter process entity locking,collision resolution and/or field level merge.
* Reference counting for persistent objects
* Intra and inter process pub/sub
* Hooks for adding undo/redo support to applications
* Add support for re-creating Database and/or PersistentCollection objects with a single invokation


