# Danake-SW

The Danake framework provides Swift application developers with tools to move data between an application and external persistent storage with thread safe and lightweight application data structures.

## Aspirations
### Initial
* Data structures which independently save and retrieve small portions of models featuring complex object graphs
* Model objects only need to implement the Codable interface; application developers will work with a standard wrapper (Entity<T>) around the model objects.
* Asynchronous and synchronous operations with thread safety designed in to normal usage patterns
* Data updates are collected into batches which are submitted automatically when they go out of scope (or manually by the application developer) with a design which prevents developers from accidently making data changes which aren’t captured in a batch.
* Database writes are eventually consistent
* Adapter architecture for supporting multiple backend databases.
* Mongo DB and in-memory (test mock) adapters
* Lazy retrieval
* Database error reporting and batch retry for the life of the process (managed by the framework)
### Eventual
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


