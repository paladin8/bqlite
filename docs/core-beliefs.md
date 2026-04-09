# Core Beliefs

These are the design principles that guide all decisions in bqlite.

1. **Performance is the top priority.** Vectorized execution, cache-aware data layouts, lock-free design, optimized compression, predicate pushdown, operator fusion. bqlite should be the fastest engine for its problem domain by a wide margin. Consider custom memory allocation (slab allocator) where it provides measurable benefit.

2. **Powerful primitives over specialized features.** The query language exposes composable primitives (sequence matching, windowing, filtering, aggregation). Funnels, retention, and cohorts are expressible as compositions of these primitives — they are convenience wrappers, not special cases. The primitives must be powerful enough to express features like holding a property constant across funnel steps, custom retention brackets, and complex session definitions. The execution engine may use specialized operators under the hood, but the language remains general.

3. **Entity-first data model.** Every query implicitly operates per-entity. The storage format, scan layer, and operators are all designed around the entity-partitioned access pattern.

4. **Clean compiler architecture.** Strict separation between the query language (frontend), logical planning, optimization, physical planning, and execution. New optimizations and physical operators can be added without changing the language.

5. **Embeddable, not a server.** bqlite is a library — `pip install bqlite` or a Rust crate dependency. No server process, no deployment, no configuration beyond pointing at a database directory. Think SQLite, not PostgreSQL.

6. **Memory-conscious.** Explicit memory budgets (default 4 GB), spill-to-disk for large intermediate results, streaming evaluation where possible. Queries over billions of events should work with bounded memory.

7. **Distributed-ready architecture.** We are not building distributed execution in v1, but the architecture should not preclude it. Physical plans should be partitionable. State should be serializable. Nothing should assume single-node.

8. **Strongly-typed pipelines.** Every operator has a well-defined input and output schema. Operators can be piped into each other with compile-time (at plan time) type checking. The planner rejects queries where schemas don't align, with clear error messages.

9. **Query expressibility.** While common query patterns should be lightning-fast, the language and operators should have high expressability and be able to compute a wide variety of aggregations. Non-aggregate selection queries should also work, but can be treated as second-class from a performance perspective.

10. **Microbenchmark frequently.** Many decisions in query execution and storage engine have non-obvious performance implications. Create microbenchmarks for any hot path code and check them after any change to make sure we're improving.
