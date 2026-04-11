# BQL Parser Grammar Framework

**Status:** accepted, Wave 2
**Task:** TASK-203
**Inputs:** `docs/design/query-language.md` (§26 grammar, §27 error strategy, §30.10 error recovery), `crates/bqlite-ast/` (AST types are already fixed)
**Unblocks:** TASK-220 (framework bootstrap + expression grammar), TASK-221 (DDL + EXPLAIN), TASK-222 (INSERT FROM), TASK-223 (pipeline + where/select/limit), TASK-238 (INSERT VALUES), and every post-stub parser task in Waves 3–4 (MATCH, FUNNEL, RETENTION, SESSIONIZE, STATS, LET, etc.)

This document pins the parser's implementation technology, error strategy, span
model, lexer contract, dispatch architecture, and the recipe for adding new
productions. It is deliberately not a grammar — §26 of the language doc is the
normative grammar and stays the single source of truth. This document is the
*how we implement §26* layer.

---

## 1. Decision Summary

| Question | Decision | Section |
|---|---|---|
| Framework | Hand-rolled recursive descent + hand-written lexer | §2 |
| External deps | None beyond `bqlite-ast` + `thiserror` | §2.2 |
| Error recovery | Halt on first error (confirms §30.10) | §4 |
| Span tracking | Byte-offset spans from lexer, propagated by parser, one `Span` per AST node | §5 |
| Lexing strategy | Eager single-pass lexer producing `Vec<Token>` before parsing | §6 |
| Keyword casing | Case-insensitive match at lex time, stored as a keyword enum variant | §6.3 |
| Name casing | Case-sensitive, byte-for-byte from source (backticks stripped) | §6.3 |
| Duration literals | Single fused lexer token (`7d`, `500ms`) | §6.4 |
| Numeric disambiguation | Longest match — duration > number > integer | §6.4 |
| Operator precedence | Pratt (precedence climbing) inside `expr` production | §7.3 |
| Production pattern | One function per production, all on a `Parser<'s>` impl | §7.1 |
| `WITH (...)` option surface | Flat literal-valued options plus a sidecar structured `map: (src AS dst, ...)` clause (AST shape landed by TASK-237 commit `8a7c7cd`) | §8 |
| Tests | Per-production example tests + one proptest per round-trippable subset | §9 |

---

## 2. Framework Choice

### 2.1 Chosen: hand-rolled recursive descent with a hand-written lexer

Every post-stub parser task (TASK-220 through TASK-223, TASK-238, plus Waves 3–4
match/funnel/stats/retention work) implements productions as plain Rust
functions on a single `Parser<'s>` type defined in
`crates/bqlite-parser/src/lib.rs`. There is no external parser generator, no
build script, and no generated source file.

**Why this wins for BQL:**

1. **Industry precedent for the exact shape we need.** Every production-grade
   SQL parser in the Rust ecosystem and beyond — `sqlparser-rs`, DataFusion's
   planner shim over `sqlparser-rs`, PostgreSQL, DuckDB, SQLite — is hand-rolled
   recursive descent. The reason is the same one §27 pins down: SQL-family
   error messages live or die on per-site hand-tuned diagnostics
   (`missing THEN between steps`, `use WHERE after STATS, not HAVING`,
   `did-you-mean SELECT`). Generated parsers hand you one-size-fits-all
   "expected X, found Y" messages that are hostile to iterative query authors.

2. **Zero runtime and build-time dependency cost.** The current
   `bqlite-parser/Cargo.toml` depends only on `bqlite-ast` and `thiserror`.
   Adding `chumsky`, `pest`, or `lalrpop` brings in a non-trivial tree of
   transitive deps and (for `lalrpop`) a build script that runs on every clean
   build. The core-beliefs doc commits us to lean dependencies; this decision
   honors that.

3. **Natural fit for context-sensitive lexing.** BQL has at least four places
   where a single pre-fused token is easier than post-hoc grammar rewriting:
   duration literals (`7d`, `500ms`), `WITHIN SESSION` (a two-word phrase
   treated as one modifier in §26), qualified identifiers via `.`, and the
   `$var` variable binding syntax. A hand-written lexer handles each of these
   in one pass with a `match` expression; a generated lexer would need tedious
   grammar contortions.

4. **Halt-on-first-error aligns with the simplest possible parser.** §30.10
   fixes error recovery at "halt on first error" for v1. The headline selling
   point of `chumsky` (principled error recovery) is therefore wasted on us.
   Every parser task can return `Result<T, ParseError>` with a plain `?`
   operator; there is no recovery-state graph to debug.

5. **Per-production evolvability.** Wave 3 adds MATCH (the most complex
   production in the language — modes, modifiers, step separators,
   parenthesized groups, variable bindings, BRACKETS, EMIT ALL). Wave 4 adds
   STATS aggregates with case-sensitive function-name resolution. Each wave
   lands as a new function on `Parser` that the existing dispatch table calls
   into. Nothing about any earlier wave's code must move. Generated grammars
   require global refactors when new productions introduce new lookahead
   requirements.

6. **Span quality is free.** Every `Token` carries its byte range. Every
   production returns a `(T, Span)` pair where `Span` is the merge of the
   spans of its constituent tokens. §27's requirement "every error includes a
   source span" is a consequence of how the parser is written rather than a
   separate concern we chase at error-handling time.

### 2.2 Alternatives considered and rejected

| Framework | Verdict | Rationale |
|---|---|---|
| `chumsky` 0.9+ | Rejected | Heavy monadic combinators; slow compile times; its killer feature (principled error recovery) is wasted under §30.10's halt-on-first-error policy; combinator types bleed into signatures and make per-production debugging painful. |
| `pest` | Rejected | External `.pest` grammar file lives separately from the AST types, so changes require coordinated edits across two files in two languages. Generated error sites lack the surgical precision §27 requires. No room to intercept tokenization for duration-literal fusing without post-processing. |
| `lalrpop` | Rejected | LR(1) generator works beautifully for clean, unambiguous grammars. BQL is LL(2) at worst *and* needs hand-tuned diagnostics; both push us away from LR-style generators. `WITHIN` vs `WITHIN SESSION`, `IN (literal-list)` vs `IN QUERY (...)` vs `IN alias`, and `NOT` in prefix vs infix positions would all require grammar rewrites. Build-time codegen adds a regrettable step to every clean build. |
| `nom` | Rejected | Parser combinators over byte slices. Error reporting requires heavy custom wrappers to reach §27's quality bar. Spans are manual. The combinator abstraction is strictly weaker than plain Rust functions for a language this size. |
| Pratt as a crate (`pratt`, `lalrpop-util::Pratt`) | Rejected | The Pratt algorithm is ~80 lines of Rust. Taking it as a dependency trades a file's worth of code for a version-pin liability. We implement it inline in `expr.rs`. |

---

## 3. Crate Layout

All parser code lives in `crates/bqlite-parser`. The Wave 1 stub at
`crates/bqlite-parser/src/lib.rs` is deleted by TASK-220 and replaced with a
module tree:

```
crates/bqlite-parser/src/
├── lib.rs            // pub fn parse(&str) -> Result<Statement, ParseError>;
│                     // pub enum ParseError; re-export of the `Parser` type
│                     // for integration tests only.
├── error.rs          // `ParseError` enum + `Expected` helper enum + `pretty`
│                     // message helpers. Pure data — no lexer/parser imports.
├── lex.rs            // `Token`, `TokenKind`, `Keyword`, `lex(&str)`.
├── parser.rs         // `Parser<'s>` type + cursor helpers + top-level
│                     // `statement()` dispatcher.
├── expr.rs           // `expression()` Pratt parser + helpers for primaries.
├── pipeline.rs       // Pipeline (source + stages) + WHERE/SELECT/LIMIT (TASK-223).
├── ddl.rs            // CREATE / ALTER / DROP / DESCRIBE / EXPLAIN (TASK-221).
├── dml.rs            // INSERT VALUES / INSERT FROM / DELETE-stub-rejected (TASK-222, TASK-238).
└── tests/            // unit tests colocated via `#[cfg(test)]` inline modules;
                      // no separate tests/ folder inside the crate.
```

Wave 3 adds `match_op.rs`, `funnel.rs`, `retention.rs`, `sessionize.rs`. Wave 4
adds `stats.rs`. Each is a new file registered in `parser.rs`'s dispatch table —
no cross-cutting refactor.

`parser.rs`, `expr.rs`, `pipeline.rs`, `ddl.rs`, and `dml.rs` are `pub(crate)`;
only `lib.rs` and `error.rs` form the public surface. The Wave 1 stub's
function signature `pub fn parse(text: &str) -> Result<Statement, ParseError>`
is preserved so `bqlite-engine` never needs to change.

---

## 4. Error Handling

### 4.1 Policy: halt on first error

§30.10 of the language doc resolves this: v1 halts on the first parse error
and reports it. No panic-mode recovery, no multiple-error diagnostics, no
synthesized placeholder nodes. This document confirms and adopts that policy.

The implication for the implementation is that every production function
returns `Result<T, ParseError>` and uses `?` to propagate. There is no separate
"error state" on the parser — the moment a production function returns `Err`,
the enclosing call site unwinds and the top-level `parse()` surface returns.

### 4.2 `ParseError` shape

```rust
// crates/bqlite-parser/src/error.rs
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ParseError {
    #[error("unexpected end of input at byte {offset}: expected {expected}")]
    UnexpectedEof {
        offset: usize,
        expected: Expected,
        /// Optional hand-tuned suggestion for §27.1 common-error categories.
        detail: Option<&'static str>,
    },

    #[error("unexpected token at byte {offset}: expected {expected}, found `{found}`")]
    Unexpected {
        offset: usize,
        line: u32,
        column: u32,
        expected: Expected,
        found: String, // the source text of the offending token
        /// Optional hand-tuned suggestion for §27.1 common-error categories
        /// — e.g., "missing pipe `|` between source and WHERE". Routed
        /// through the formatter so users see both the category-level
        /// "expected PipelineStage" and the site-specific suggestion.
        detail: Option<&'static str>,
    },

    #[error("invalid {kind} literal at byte {offset}: {reason}")]
    InvalidLiteral {
        offset: usize,
        kind: LiteralKind, // Int | Float | String | Duration | Timestamp
        reason: String,
    },

    #[error("unterminated {kind} at byte {offset}")]
    Unterminated {
        offset: usize,
        kind: UnterminatedKind, // BacktickIdent | StringLit | BlockComment
    },

    #[error("reserved keyword `{keyword}` cannot be used as a {role}")]
    ReservedKeyword {
        offset: usize,
        keyword: &'static str,
        role: NameRole, // TableName | ColumnName | AliasName | VariableName | StepName
    },
}
```

The `Expected` helper enum is a closed vocabulary of parser-facing expectations
— `Keyword(&'static str)`, `Punct(&'static str)`, `Identifier`, `Literal`,
`Expression`, `ColumnDef`, `PipelineStage`, etc. Having a finite set (rather
than free-form strings) keeps the *category* of the error consistent across
sites and lets TASK-221/222/223 reuse the same variants without inventing new
phrasings. Per-site nuance lives in the `detail: Option<&'static str>` field
on `Unexpected` / `UnexpectedEof`, where `None` means "default message" and
`Some(..)` supplies the §27.1 suggestion heuristic. Keeping the detail
strings `&'static str` is a deliberate restriction: it rules out dynamic
interpolation and forces each call site to pick a canned message, which keeps
diagnostics reviewable at parser-code-review time rather than drifting into
freeform prose.

Every variant carries a byte offset; variants raised from lexer errors also
carry `line` and `column` so the REPL/CLI can render a caret without rescanning
the source. Byte offsets are always `start` (not `end`) — error messages point
at the first unexpected byte.

### 4.3 Error-message quality expectations

Each production that implements a §27.1 common-error category owns a custom
error *site* — not a new `ParseError` variant, but a specific
`Expected` + `detail` combination surfaced at the right call site. The
`detail` field on `Unexpected` / `UnexpectedEof` carries a static suggestion
string that the CLI renders alongside the category message. Examples that
TASK-220 through TASK-223 are on the hook to deliver:

| §27.1 Category | Call site | `Expected` | `detail` |
|---|---|---|---|
| Missing pipe | `pipeline::parse_stage()` after a `WHERE` keyword following a source | `Expected::Punct("|")` | `"missing pipe \`|\` between source and WHERE"` |
| Unknown operator | top-level `statement()` after an identifier that looks like a keyword | `Expected::PipelineStage` | `None` — the CLI layer computes a Levenshtein-1 did-you-mean over the reserved-keyword list at render time; the parser does not bake it into the error value |
| Missing THEN | Wave 3 `match_op::step_list()` | `Expected::Keyword("THEN")` | `"step separator THEN or -> expected between steps"` |
| Trailing WITHOUT | Wave 3 `match_op::step_sep()` | `Expected::EventRef` | `"WITHOUT must appear between two steps"` |
| Shadowed keyword | any production that consumes a bare name | — | `ParseError::ReservedKeyword` (dedicated variant, no `detail`) |

Wave 2's TASK-220 lands only the "missing pipe" and "unknown operator" sites
from the table above; later waves add their own. The key commitment is that
every new production that introduces a common-error category is accompanied by
its hand-tuned error site in the same PR — not as a Wave 7 polish pass.

### 4.4 What error recovery *looks* like in v1

In practice, the Wave 1 stub's five-variant `ParseError`
(`Empty`, `Syntax`, `TrailingInput`, `UnterminatedBacktick`, `EmptyBacktick`
— see `crates/bqlite-parser/src/lib.rs:59–97`) is the template: the user
sees exactly one error, exactly one source span, exactly one suggestion,
and the parser returns. The CLI and the builder APIs print the error and
exit; they do not attempt to continue parsing or to walk the partial AST.
If iterative query authors find this inadequate, a Wave 6+ polish task may
revisit §30.10. The §4.2 variant set replaces the Wave 1 stub's variants
wholesale — the stub's names were shaped around the single-identifier
grammar and do not carry forward.

---

## 5. Span Tracking

### 5.1 Every token carries a span

The lexer produces `Vec<Token>` where each token is:

```rust
// crates/bqlite-parser/src/lex.rs
pub(crate) struct Token {
    pub kind: TokenKind,
    pub start: usize, // byte offset (inclusive)
    pub end: usize,   // byte offset (exclusive)
    pub line: u32,    // 1-indexed line of `start`
    pub column: u32,  // 1-indexed column of `start`
}

pub(crate) fn token_span(t: &Token) -> bqlite_ast::Span {
    bqlite_ast::Span::new(t.start, t.end, t.line, t.column)
}
```

The lexer scans the source once and tracks `(line, column)` as it goes,
incrementing `line` on every `\n` and resetting `column` to 1. Multi-byte UTF-8
characters (inside string literals and backtick identifiers) advance `column`
by one per Unicode codepoint, not per byte, so carets land correctly in CLI
output. Outside string literals BQL is ASCII-only, so the fast path avoids
UTF-8 decoding.

### 5.2 Every production returns `(Node, Span)` implicitly

Every production function captures the first token's span before it advances
the cursor and merges in the span of whatever ends the production. The
`expect_kw` helper returns a `Token`; the `lex::token_span` helper from §5.1
turns it into a `bqlite_ast::Span`; `expect_int` returns the value and its
span as a tuple; and `PipelineStage::Limit` stores its count as `u64`:

```rust
fn parse_limit(&mut self) -> Result<PipelineStage, ParseError> {
    let start = self.expect_kw(Keyword::Limit)?;              // Token
    let (count_value, count_span) = self.expect_int()?;       // (i64, Span)
    // The lexer only emits non-negative Int tokens (§6.4 — negative
    // literals are unary minus applied to positives), so this cast is
    // safe. A negative literal becomes an `Unexpected` at the preceding
    // `-` token, not a wrap here.
    debug_assert!(count_value >= 0);
    let span = crate::lex::token_span(&start).merged(count_span);
    Ok(PipelineStage::Limit {
        count: count_value as u64,
        span,
    })
}
```

`Span::merged` already exists in `bqlite-ast/src/span.rs` (TASK-106 landed it).
This design reuses it — no new span infrastructure is introduced.

### 5.3 Line/column are set exactly once — at lex time

The parser never recomputes `(line, column)`. Once a token has its span set,
all downstream spans inherit from token spans via `merged`. This rule means
the parser can do all its work on byte offsets alone, and the line/column
cache survives the conversion from token spans to AST spans without being
re-derived.

---

## 6. Lexer Design

### 6.1 Shape

The lexer is a pure function:

```rust
pub(crate) fn lex(source: &str) -> Result<Vec<Token>, ParseError>;
```

Takes a borrowed source string, returns a `Vec<Token>` containing an explicit
`TokenKind::Eof` terminator. On failure returns `ParseError::Unterminated`,
`ParseError::InvalidLiteral`, or `ParseError::Unexpected` (for lex-level junk
bytes). No partial returns — the lexer either produces the full stream or
errors.

Eager one-pass lexing is chosen because:

1. BQL inputs are short (typically < 10 KB — REPL queries are a few hundred
   bytes; the largest inputs are multi-statement scripts). Materializing the
   whole token stream in a `Vec` is free.
2. The parser wants arbitrary lookahead (e.g., `WITHIN` vs `WITHIN SESSION`)
   without bookkeeping. A `Vec<Token>` + cursor is the simplest form of
   arbitrary lookahead.
3. Tests become trivial — lexer output is snapshot-inspectable.
4. Duration-literal fusing (§6.4) is naturally expressed as lookahead inside
   the numeric-literal branch, which is easier with a pre-tokenized stream.

### 6.2 Token kinds

```rust
pub(crate) enum TokenKind {
    // Literals
    Int(i64),
    Number(f64),
    Duration(i64),   // nanoseconds
    Timestamp(i64),  // nanoseconds since epoch (for `@2024-01-01T...` literals)
    String(String),  // interpreted string literal, escapes resolved
    Bool(bool),
    Null,

    // Identifiers and names
    Ident(String),       // bare [a-zA-Z_][a-zA-Z0-9_]*
    QuotedName(String),  // contents of backtick-quoted name, backticks stripped
    Variable(String),    // `$foo` → text "foo"

    // Keywords
    Kw(Keyword),

    // Punctuation (single-char unless noted)
    LParen, RParen,
    LBracket, RBracket,
    Comma,
    Dot,
    Colon,
    Pipe,          // `|`
    Semicolon,
    Eq,            // `=`
    NotEq,         // `!=` or `<>`
    Lt, LtEq,
    Gt, GtEq,
    Plus, Minus, Star, Slash, Percent,
    RegexMatch,    // `~=`
    Arrow,         // `->`

    // Terminators
    Eof,
}
```

`Keyword` is a dedicated C-like enum mirroring §26.2's reserved keyword list
exactly:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Keyword {
    Add, And, All, Alter, As, Asc, Attribute, Avg, Between, Brackets, By,
    Case, Cast, Coalesce, Column, Contains, Count, CountDistinct, Create, Cumulative,
    Default, Delete, Desc, Describe, Distinct, Drop,
    Else, Emit, End, Entity, Event, Explain,
    False, First, From, Funnel,
    Group,
    If, Immediately, In, Insert, Into, Is,
    Join,
    Key,
    Lag, Last, Lead, Length, Let, Like, Limit, List,
    Map, Match, Max, Min,
    Not, Nth, Null,
    On, Or, Order, Over,
    P50, P90, P95, P99, Partition, Pivot,
    Query,
    Rank, Retention, RowNumber,
    Sample, Sequence, Select, Session, Sessionize, Stats, Sum,
    Table, Then, Time, Timestamp, True, Type,
    Upper,
    Values,
    When, Where, With, Within, Without,
    // scalar type keywords
    Bool_, Int_, Float_, String_,
}
```

The trailing underscores on type keywords avoid collision with the Rust
primitive type names inside the parser crate. `COUNT_DISTINCT` and `ROW_NUMBER`
are underscore-removed when matching but the enum variants are CamelCase.

### 6.3 Keyword resolution and identifier casing

The lexer reads bare identifiers into a `String`, then:

1. **If the backticks are present**, the token is `QuotedName(inner)` where
   `inner` is the scanned identifier body with backtick doubling resolved:
   query-language.md §23.2 pins `` `a``b` `` → literal identifier `` a`b ``.
   The lexer scans until a single (unpaired) backtick is found; a paired
   backtick (`` `` ``) is consumed as one literal backtick character and
   scanning continues. Wave 1's stub does not support doubling — it closes on
   the first inner backtick — and §4.4 calls out that Wave 2 replaces the
   stub's variants wholesale. There is no keyword lookup on quoted names —
   quoted identifiers never match keywords by themselves (§26.3 rule 2).
   **Exception: keyword shadowing.** §26.3 rule 3 still rejects `` `MATCH` ``
   when used as a table/column/alias name — the lexer accepts it as a
   `QuotedName`, the parser accepts it as a name, and the **planner** raises
   the collision error (so the parser does *not* emit `ReservedKeyword` for
   backtick-wrapped keyword names — only for bare keyword identifiers). If
   `inner` is empty after doubling resolution, emit
   `Unterminated { kind: BacktickIdent }` for a lone unterminated backtick or
   `InvalidLiteral { kind: String, reason: "empty backtick identifier" }`
   for `` `` ``.
2. **Otherwise**, ASCII-uppercase the identifier text and look it up in a
   `phf`-less static table (`&[(&str, Keyword)]`). A linear scan over ~90
   entries is faster than the hash overhead for BQL's short inputs. If the
   lookup hits, emit `TokenKind::Kw(...)`. Otherwise emit `TokenKind::Ident`
   carrying the *original* (case-preserved) text. This honors §26.3's rule:
   user identifiers are case-sensitive; keywords are case-insensitive.
3. **Variable references** (`$foo`) are tokenized by consuming the `$` and
   then the identifier body; the variable name is also stored case-preserved.
4. `COUNT_DISTINCT` tokenizes as a single `Ident("COUNT_DISTINCT")`, is
   ASCII-uppercased to `"COUNT_DISTINCT"`, and maps to `Kw::CountDistinct`.
   `ROW_NUMBER` is the same pattern.

Scalar function names (`QUANTIZE`, `CONCAT`, etc.) are not in the keyword
table — they are plain `Ident`s. The parser resolves them to function calls
at parse time by looking at the trailing `(`. This keeps them out of the
reserved list and matches §26.2's "scalar function names are not reserved"
rule.

### 6.4 Number, duration, and timestamp disambiguation

The numeric literal branch is the lexer's most subtle spot. §26.1's
longest-match rule is implemented as:

```text
1. Consume a run of ASCII digits (at least one).
2. If the next char is a valid duration-unit prefix
   (ns | us | ms | s | m | h | d), consume the longest matching unit
   suffix and emit Duration(nanos). The unit set is the one pinned by
   query-language.md §22 — no additions without a §22 edit.
3. Otherwise if the next char is '.', consume another run of digits and
   emit Number(f64).
4. Otherwise emit Int(i64). Overflow produces InvalidLiteral.
```

Notes:

- Step 2's unit table is literally the one in §22 of the language doc. New
  units require both a lexer change and a language-doc edit; the proptest in
  §9 guards against drift by enumerating the unit set.
- Step 3 does not accept scientific notation in Wave 2. Scientific notation is
  added in Wave 3 if any user query needs it. This is consistent with the
  existing AST: `Literal::Float(f64)` already covers the storage shape.
- Step 4 uses `i64::from_str_radix` + overflow check. Literals larger than
  `i64::MAX` produce `InvalidLiteral`, not a silent wrap.
- A leading minus is *not* part of the numeric literal — §26.1 pins negative
  numbers as unary minus on top of a positive literal, so `LIMIT -5` is
  correctly rejected because `expect_int()` refuses to match a `Minus` token.

Timestamp literals (`@2024-01-01T00:00:00Z`) are tokenized by recognizing a
leading `@` followed by an ISO-8601 pattern up to the first whitespace or
punctuation that cannot appear in a timestamp. The `chrono` crate already
lives in the workspace, and Wave 2's `Literal::Timestamp(i64)` stores
nanoseconds. Wave 2 opts to **defer timestamp literal tokenization to
TASK-220** (pilot) and **TASK-221** (DDL) — the only Wave 2 use sites are
`BETWEEN 'ts1' AND 'ts2'` string forms, which tokenize as ordinary string
literals and are parsed into timestamps at plan time (matching the AST's
`TimeRange::Between { start: String, end: String }` shape). No `@`-prefixed
literals are required for the Wave 2 acceptance query.

### 6.5 String literals and escapes

Single-quoted strings follow SQL convention: `''` is an escaped single quote.
No `\n`-style backslash escapes in v1 — SQL-style doubling is the only escape.
This matches §26 grammar line 1657 (`string_lit := "'" [^']* "'"`) with the
clarifying note that the lexer upgrades the regex to `('' | [^'])*` to handle
doubling.

Double-quoted strings are **not** valid identifiers or string literals in BQL
v1 — §23 uses backticks for quoting. A `"` character encountered outside a
string literal is a lex error (`Unexpected` with `Expected::Literal` or
`Expected::Identifier` depending on context). Inside a `'...'` string literal
the `"` character is, of course, an ordinary character and is preserved
verbatim in the decoded string.

### 6.6 Comments

Line comments `-- …\n` and block comments `/* … */` are stripped by the
lexer and never produce tokens. Nested block comments are **not** supported
in v1 — the first `*/` ends the comment. Unterminated block comments raise
`Unterminated { kind: BlockComment }`.

### 6.7 Whitespace and newlines

All ASCII whitespace is treated identically for token separation. The lexer
tracks `(line, column)` internally so spans on downstream tokens are correct,
but no token is emitted for whitespace.

---

## 7. Parser Architecture

### 7.1 `Parser<'s>` type

```rust
// crates/bqlite-parser/src/parser.rs
pub(crate) struct Parser<'s> {
    source: &'s str,
    tokens: Vec<Token>,
    cursor: usize,
}

impl<'s> Parser<'s> {
    pub(crate) fn new(source: &'s str) -> Result<Self, ParseError> {
        let tokens = crate::lex::lex(source)?;
        Ok(Self { source, tokens, cursor: 0 })
    }

    // Lookahead helpers — never advance the cursor.
    fn peek(&self) -> &Token;                 // returns Eof at end
    fn peek_kind(&self) -> &TokenKind;        // convenience for match
    fn peek_at(&self, n: usize) -> &Token;    // n-token lookahead

    // Mutating helpers — advance the cursor.
    fn bump(&mut self) -> Token;              // unconditional advance
    fn try_kw(&mut self, k: Keyword) -> Option<Token>;
    fn try_kind(&mut self, k: &TokenKind) -> Option<Token>;
    fn expect_kw(&mut self, k: Keyword) -> Result<Token, ParseError>;
    fn expect_punct(&mut self, p: TokenKind) -> Result<Token, ParseError>;
    fn expect_ident(&mut self) -> Result<(String, Span), ParseError>;
    fn expect_name(&mut self) -> Result<Name, ParseError>; // bare OR quoted
    fn expect_int(&mut self) -> Result<(i64, Span), ParseError>;

    // Diagnostic helpers.
    fn error_unexpected(&self, expected: Expected) -> ParseError;
}
```

Every field is private. Productions live in sibling modules and take
`&mut Parser` by reference; they call only the helper methods above. This
contract is what lets later waves add new modules without touching earlier
ones.

### 7.2 Top-level dispatch

`lib.rs`:

```rust
pub fn parse(source: &str) -> Result<Statement, ParseError> {
    let mut p = parser::Parser::new(source)?;
    let stmt = parser::statement(&mut p)?;
    parser::expect_eof(&mut p)?;
    Ok(stmt)
}
```

`parser::statement` is a dispatch table keyed on the first token:

```rust
pub(crate) fn statement(p: &mut Parser) -> Result<Statement, ParseError> {
    match p.peek_kind() {
        TokenKind::Kw(Keyword::Create)    => ddl::parse_create_table(p),
        TokenKind::Kw(Keyword::Drop)      => ddl::parse_drop_table(p),
        TokenKind::Kw(Keyword::Alter)     => ddl::parse_alter_table(p),
        TokenKind::Kw(Keyword::Describe)  => ddl::parse_describe(p),
        TokenKind::Kw(Keyword::Explain)   => ddl::parse_explain(p),
        TokenKind::Kw(Keyword::Insert)    => dml::parse_insert(p),
        TokenKind::Kw(Keyword::Delete)    => dml::parse_delete(p),

        // Everything else is either an alias definition or a query pipeline.
        // Alias definitions start with `identifier "=" pipeline`; the
        // dispatcher disambiguates by peeking at the 2nd token for `=`.
        _ => query_or_alias(p),
    }
}
```

`query_or_alias` peeks two tokens ahead — `Ident` followed by `Eq` means it's
an alias definition (`name = pipeline`); otherwise it falls through to
`pipeline::parse_pipeline`. Lookahead-2 is the deepest any Wave 2 production
needs; `peek_at(n)` supports it.

### 7.3 Expression grammar: Pratt climbing

Expressions use Pratt / precedence-climbing inside `expr.rs`. The precedence
table mirrors §26's `or_expr → and_expr → not_expr → comparison → addition →
multiplication → unary → primary` ladder:

| Level | Op | Associativity |
|---|---|---|
| 1 (lowest) | `OR` | left |
| 2 | `AND` | left |
| 3 | `NOT` (prefix) | right |
| 4 | `= != < <= > >= IS IS NOT IN NOT IN BETWEEN LIKE NOT LIKE ~= CONTAINS` | non-assoc |
| 5 | `+ -` (binary) | left |
| 6 | `* / %` | left |
| 7 (highest) | `-` (prefix unary) | right |
| atom | literal, name, qualified name, function call, `(expr)`, `CASE`, `CAST`, `$var` | — |

The implementation is the standard "recursive descent with precedence
climbing" pattern: `parse_expr_bp(min_bp: u8)` recurses on the RHS with the
operator's binding power. Right-associative operators (unary `NOT`, unary
`-`) use `min_bp + 1` on the recursive call; left-associative operators
use `min_bp` and the outer loop re-enters.

Comparison operators are non-associative — `a = b = c` is a parse error, not
a chain. This matches SQL semantics and the `comparison` grammar production
(§26) which uses `addition (comp_op addition)?` with a single optional
comparison step.

The `NOT IN`, `NOT BETWEEN`, `NOT LIKE` forms are implemented as a tiny
left-context hack: inside `comparison()`, after consuming the LHS, we peek for
`NOT` followed by `IN`/`BETWEEN`/`LIKE` and consume both as a single
"negated comparison" branch. This matches §26.1 line 1678 exactly.

### 7.4 Limits on lookahead

No production needs more than 2 tokens of lookahead in Wave 2. Wave 3's MATCH
step separators push this to 3 in one spot (`WITHOUT <event_ref> THEN`).
`peek_at(n)` scales linearly with `n`; since `n ≤ 3` forever, this is free.

The parser never **mutates** a token it has only peeked at — `peek_at` returns
a borrow, `bump`/`try_kw`/`expect_kw` are the only mutators. This invariant is
what makes partial production state impossible: every production either
commits (advances) or returns an error (leaves the cursor where it was).

### 7.5 No backtracking

Hand-rolled recursive descent *can* backtrack by saving and restoring
`self.cursor`. Wave 2 forbids this. Every production commits or errors after
at most `peek_at(k)`-style lookahead; cursor is never rewound. The rule exists
because backtracking destroys the error-quality guarantee: a backtracked
failure hides the inner error behind a less-specific outer one.

If a future production needs lookahead-based disambiguation deeper than
`peek_at(3)` allows, the fix is **more peek, not rewind**. When the grammar
is inherently ambiguous with finite lookahead, the resolution lives in the
language doc — the parser is not the place to resolve spec ambiguity.

---

## 8. `WITH (...)` Option List Surface

### 8.1 Why this surface lives in the framework doc

§20.1 and the Wave 2 acceptance script bake in a specific `WITH (...)` shape:

```bql
INSERT INTO purchases FROM 'data.csv'
WITH (format: 'csv',
      delimiter: ',',
      header: true,
      map: (uid AS user_id, time AS ts, evt AS event));
```

Two distinct grammatical novelties live inside this clause:

1. **Colon-separated key/value pairs.** `key: value` rather than `key = value`.
   This is the only place in BQL where `:` carries a key/value meaning at the
   statement surface.
2. **Structured values.** Most options (`format`, `delimiter`, `header`) take a
   literal right-hand side. But `map` takes a structured column-mapping list
   `(src AS dst, src AS dst, ...)` that cannot be represented as a
   `Literal`.

TASK-237 owned the AST-shape decision for (2) and landed a parallel-field
shape on `InsertBody::From` (see §8.3). TASK-203 owns the *parser surface*
decision — the concrete productions TASK-222 will implement against the
AST shape TASK-237 landed. Those two decisions are independent in principle
and were sequenced so the AST shape landed first; §8.2 fixes the surface,
§8.3 adapts the productions to the AST.

### 8.2 Surface productions

```text
with_clause      := "WITH" "(" option_list ")"
option_list      := option ("," option)* ","?      // trailing comma allowed
option           := identifier ":" option_value
option_value     := literal                         // format / delimiter / header / ...
                  | column_mapping_list             // map: (...)
column_mapping_list := "(" column_mapping ("," column_mapping)* ","? ")"
column_mapping   := identifier "AS" identifier      // src AS dst
```

Notes:

- **Key identifier.** Option keys are always plain `Ident` tokens. Reserved
  keywords cannot be option keys — using `format: 'csv'` is fine but
  `table: 'x'` would raise `ReservedKeyword` because `TABLE` is in the
  §26.2 list. Option keys are case-sensitive for comparison by the engine,
  matching how column names resolve elsewhere (§26.3).
- **Trailing comma.** Allowed for both `option_list` and `column_mapping_list`
  to match the conveniences users expect from modern config syntaxes. This is
  a parser-only decision — the AST has no "trailing comma was present" bit.
- **Value disambiguation.** The parser peeks the token after `:` to decide
  which arm to take. `LParen` → structured; anything else → literal. This
  means only structured-value options need the parenthesized form; all other
  options can be plain literals.
- **Which keys accept structured values.** Wave 2 ships with exactly one
  structured-value key: `map`. Future options may add more (e.g., a
  `columns: (...)` form for explicit column listings), but doing so is a
  language-doc change *plus* a new arm in `option_value`, not a grammar-wide
  rewrite. Validation that `map` is the only legal key for the structured
  form lives in the planner (TASK-226), not the parser — the parser accepts
  any `identifier : (mapping_list)` and lets the planner reject unknown
  structured-value keys with a typed error. This is the same pattern used
  for unknown literal keys today.
- **`AS` direction.** `src AS dst` means "the source column `src` maps to
  the table column `dst`". This matches the user-facing example in §20.1
  exactly and is the inverse of the shorthand some SQL dialects use for
  column aliasing — we pick the `source AS destination` direction because
  that reads in "natural English" inside an ingest context.

### 8.3 AST interface (the shape TASK-237 landed)

TASK-237 (commit `8a7c7cd`) landed the following AST shape for
`InsertBody::From`:

```rust
pub enum InsertBody {
    Values(Vec<Vec<Literal>>),
    From {
        path: String,
        options: Vec<InsertOption>,          // flat literal-valued options
        map: Option<Vec<ColumnMapping>>,     // structured column-rename clause
    },
}
pub struct InsertOption { pub key: Name, pub value: Literal, pub span: Span }
pub struct ColumnMapping { pub source: Name, pub target: Name, pub span: Span }
```

TASK-237 chose the **parallel-field** shape — a dedicated `map` field on the
`From` variant, sitting alongside the existing flat `options` list — rather
than widening `InsertOption::value` into an enum. The rationale TASK-237
records in its commit message:

> widening `InsertOption::value` to admit lists would force every option
> consumer to pattern-match even when it only cares about literal-valued
> keys.

That's a defensible call for a single structured-value key: TASK-232 /
TASK-233's option consumers iterate the flat `options` list looking for
`format`, `delimiter`, `header`, and similar literal keys; widening those
consumers to handle an enum that could also carry a column-mapping list
would add pattern-match noise for no gain as long as `map` is the only
structured-value key in v1. If a future option ever needs a second
structured-value shape, the decision can be revisited at that point —
either by adding a second parallel field or by introducing the enum then.

**Implications for the parser productions in §8.2:**

- `parse_with_clause` produces `(Vec<InsertOption>, Option<Vec<ColumnMapping>>)`
  rather than a single `Vec<Option>`. The outer `option_list` production
  accumulates literal-valued options into the flat list and routes the
  `map:` entry into the separate return value.
- The production *rejects* multiple `map:` entries in the same `WITH (...)`
  clause with a dedicated error (`ParseError::Unexpected` carrying
  `detail: Some("duplicate map clause")`) rather than silently keeping the
  last one.
- An empty `map: ()` (zero entries between the parentheses) is also a parse
  error (`detail: Some("empty map clause")`) — TASK-237's AST comment notes
  that "parsed programs never produce `Some(vec![])`" even though the type
  would admit it.
- `ColumnMapping`'s fields are named `source` and `target` in the AST, *not*
  `src` and `dst`. The user-facing grammar in §8.2 still reads
  `src AS dst` — those are just the identifier *labels* users write in
  source text. TASK-222's parser maps the left `identifier` →
  `ColumnMapping.source` and the right `identifier` → `ColumnMapping.target`.
- **Literal-option keyword collisions.** TASK-237's `InsertOption.value` is
  still a plain `Literal`, so key collisions between a legitimate option
  name (`format`, `delimiter`, `header`) and a reserved keyword (`TABLE`,
  `FROM`, `VALUES`) are caught by the §4.2 `ReservedKeyword` variant when
  the key is lexed as a `Kw(...)` token. The parser surfaces the specific
  offending keyword, not a generic "unexpected token."

---

## 9. How to Add a New Production

Every parser task in Wave 2+ follows the same recipe. Each step is a hard
requirement — skipping any of them produces review blockers.

1. **Find the grammar production in §26.** Copy the production's EBNF line
   into a doc comment on the new function so the mapping is visible without
   opening the language doc.
2. **Pick the module.** Use the table in §3. New wave-scoped productions go
   into a new module (`match_op.rs`, `funnel.rs`, etc.). Do not cross-link
   between production modules — all shared helpers live on `Parser<'s>` or
   in `expr.rs` (for expression-reusing productions).
3. **Write the function signature.** Every production function has the shape
   `fn parse_<name>(p: &mut Parser) -> Result<<Node>, ParseError>`. The
   return type is the AST node, never wrapped in `Option` — missing optional
   sub-productions are handled inside, not at the call site.
4. **Capture the start span first.** Grab a clone of `p.peek().span` before
   the first `expect_*` call. This is the production's span anchor; merge the
   end span into it as the production consumes tokens.
5. **Use helpers, not raw cursor moves.** `try_kw` for optional keywords,
   `expect_kw` for required ones, `expect_punct` for punctuation,
   `expect_ident`/`expect_name` for identifiers, `expect_int` for integer
   literals. These helpers normalize error shapes across the parser.
6. **Add the dispatch arm.** Register the new production in
   `parser::statement` (top-level) or the relevant sibling dispatcher
   (e.g., `pipeline::parse_stage` for a new pipeline verb). If the new
   production needs a helper method that is useful to more than one call
   site (e.g., `expect_type_expr` for DDL and for `CAST(... AS T)`), that
   helper lives on `impl Parser<'s>` in `parser.rs` next to the existing
   `expect_*` family. Helpers that are useful to exactly one production
   stay private to that production's module.
7. **Write three tests per production, minimum.** One happy path, one
   error path targeting §27.1's relevant category, and one span-preservation
   assertion.
8. **Touch the language doc if and only if the production surface changes.**
   The grammar in §26 is normative; when a new production is added or an
   existing one gains a form, update §26 in the *same checkpoint* as the
   parser change. Drift between the grammar doc and the parser is a
   review-blocker.
9. **If the production can be round-tripped, add a proptest.** See §10.

New reserved keywords added by a production follow the same rule:
update §26.2 in the same checkpoint. The lexer's keyword table is the
executable form of §26.2 and must not drift from it.

---

## 10. Testing Pattern

### 10.1 Unit tests per production

Every module gets an inline `#[cfg(test)]` module. The canonical shape follows
the Wave 1 stub (`crates/bqlite-parser/src/lib.rs` tests block):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_where_with_binary_predicate() {
        let stmt = parse("events | where amount > 100").unwrap();
        // shape assertions...
    }

    #[test]
    fn rejects_where_without_pipe() {
        match parse("events where amount > 100") {
            Err(ParseError::Unexpected { expected, .. }) => {
                assert!(matches!(expected, Expected::Punct("|")));
            }
            other => panic!("expected missing-pipe error, got {other:?}"),
        }
    }
}
```

### 10.2 Property tests — one per round-trippable subset

TASK-124 landed a property-test harness in `tests/src/strategies.rs`. TASK-220
extends it with an expression-AST strategy (`arb_expr()`) and an identifier
strategy (`arb_ident()`). The single canonical parser proptest is:

> **For any AST expression that can be printed as source text, parsing that
> text reproduces the same AST (up to spans).**

This is a classic printer/parser round-trip. It catches:

- Operator precedence bugs (a printed expression wraps lower-precedence
  children correctly and the parser re-associates them identically).
- Whitespace-insensitivity drift.
- Keyword-vs-identifier confusion (the printer emits reserved keywords in
  backticks when they show up as names).
- Literal escaping drift (single-quote doubling, duration unit encoding).

Only the round-trippable subset of the AST gets a proptest. Expressions
(`Expr`), literals (`Literal`), `ColumnDef`, and `PipelineStage` are all
round-trippable. Pipelines, alias definitions, and full statements are not —
the grammar has enough surface syntax (`|`, trailing `;`, optional WITH
clauses) that a printer becomes its own project.

This is *one* proptest, not one per production. Per the agent operating
protocol's "when to reach for property tests" rule: a small number of deep
invariants beats dozens of shallow ones.

### 10.3 Fuzz list (deferred)

A fuzz target for `parse(&str)` is not in scope for Wave 2. It belongs to
Wave 5's reliability work and would live in
`crates/bqlite-parser/fuzz/fuzz_targets/parse.rs` under `cargo-fuzz`. The
Wave 2 property test covers the same correctness surface for structured
inputs; fuzzing adds value only for unstructured byte-level inputs, which is
a different concern than grammar correctness.

---

## 11. Wave 2 Scope Breakdown

Each Wave 2 parser task inherits specific rows of this framework. The table
is normative — if a parser task's output does not match its row, it is
blocked on revising this doc first.

| Task | New modules | New helpers on `Parser` | Lands in lexer | Error sites |
|---|---|---|---|---|
| TASK-220 | `lex.rs`, `parser.rs`, `expr.rs` | all of §7.1 | full §6 token set minus timestamp literal | unexpected-token, unterminated string, unterminated backtick, reserved keyword, operator precedence errors |
| TASK-221 | `ddl.rs` | `expect_type_expr`, `parse_column_modifier` | — | duplicate role on column, missing type in column def, `IF EXISTS` not supported (suggest removal), `ADD COLUMN` missing COLUMN keyword |
| TASK-222 | `dml.rs` (`parse_insert_from`) | `parse_with_clause`, `parse_option_value`, `parse_column_mapping_list` | — | malformed mapping entry, duplicate `src` names, missing WITH after path, unknown `format` value (planner-level, parser only surfaces "not a known literal") |
| TASK-223 | `pipeline.rs` | `parse_pipeline_stage` | — | missing pipe, unknown stage keyword (did-you-mean) |
| TASK-238 | `dml.rs` (`parse_insert_values`) | — | — | wrong arity, empty VALUES list, trailing comma allowed but empty tuple rejected |

TASK-220 does the heaviest framework lift — its deliverable is the crate
skeleton plus the expression grammar. Everything after TASK-220 adds a
sibling module and a dispatch arm.

### 11.1 Non-goals for Wave 2

- **Multi-statement scripts** (`| alias = ... ; events ...`). §22 of the
  language doc permits multi-statement alias scripts, but Wave 2's AST does
  not yet ship `Vec<Statement>` batching and Wave 2's engine does not yet
  process multi-statement input. The parser produces exactly one
  `Statement` per `parse` call. Attempting two statements in a single call
  raises `ParseError::Unexpected` at the first token after the first
  statement completes, with the expected shape `Expected::Eof`.

- **MATCH, FUNNEL, RETENTION, SESSIONIZE, STATS, LET, PIVOT, ATTRIBUTE.**
  These are Wave 3 / Wave 4 productions. TASK-223's stage dispatcher
  includes an explicit "unknown stage — this verb lands in Wave 3 (MATCH),
  Wave 3 (FUNNEL), Wave 4 (STATS), ..." arm that maps each known-but-unsupported
  verb to a helpful error rather than a generic unknown-keyword error.
  This costs one `match` arm per deferred verb and pays off when users
  iterate on the language surface before the implementation catches up.

- **Scientific-notation number literals** (`1e6`, `2.5e-3`). Deferred to
  Wave 3 pending a use case.

- **Timestamp literals** (`@2024-01-01T00:00:00Z`). Deferred per §6.4. The
  `BETWEEN 'ts1' AND 'ts2'` form covers the Wave 2 acceptance test via
  string-literal parsing at plan time.

- **`DELETE` parsing.** Wave 2 scope exclusion: `DELETE` pairs with
  tombstones and ships in Wave 4. TASK-221's DDL dispatcher explicitly
  rejects `DELETE` with a "DELETE is not supported in this version"
  error — not an unknown-keyword error — so users understand the gap.

---

## 12. Open Questions (non-blocking)

1. **Phrase-keyword normalization.** `WITHIN SESSION` is two tokens in the
   lexer (`WITHIN` keyword + `SESSION` keyword) but is one semantic modifier.
   Wave 3's MATCH production handles the two-token sequence with a 2-token
   peek and returns a single modifier node. No lexer change is needed;
   documenting for Wave 3 agents.
2. **Name-keyword escape hatch.** A small number of keywords that also appear
   in §26.2's reserved list can legitimately show up in primary-expression
   position as function-call heads or as references — `UPPER`, `LENGTH`, and
   similar scalar functions listed in the closing paragraph of §26.2 are
   *not* reserved (so `round(x)` is legal), but `FIRST` and `LAST` *are*
   reserved keywords because they can appear as bare operator tokens. When
   `Kw(First)` is followed by `(`, the parser must recognize the function-call
   shape from §26's `first_last_op` production, not treat the `(` as a syntax
   error. This is handled inside `expr.rs::primary()` and its pipeline-stage
   counterpart by dispatching on the keyword variant when the trailing token
   is `LParen`. Double-check with TASK-220 for the scalar-function case and
   with Wave 3's MATCH task for the entity-operator case.
3. **Semicolon tolerance.** The Wave 2 acceptance script uses trailing `;`
   as a statement terminator in multiple places (e.g.,
   `CREATE TABLE ... ;`). Wave 2 opts to **accept and ignore** a trailing
   `;` at the end of a single-statement `parse` call, for ergonomics. This
   is a one-line `try_kind(TokenKind::Semicolon)` at the end of
   `parser::statement()`. The AST does not record that a `;` was present.
4. **`DROP TABLE IF EXISTS`.** §20.4 and §26 line 1643 pin the absence of
   `IF EXISTS`. TASK-221 must raise a typed error with the `detail`
   suggestion "DROP TABLE has no IF EXISTS form in bqlite" rather than a
   generic unexpected-keyword error. The error-site catalog in §4.3
   tracks this.

---

## 13. Summary

Hand-rolled recursive descent with a custom lexer is the simplest parser
design that lets us hit §27's error-quality bar without paying dependency,
build-time, or abstraction costs. Every production function is plain Rust,
every error carries a span, and every new wave adds one module without
touching the framework. The halt-on-first-error policy from §30.10 is
honored by the use of `Result` + `?` throughout — no recovery state, no
partial trees, no phantom errors.

The only non-trivial design call is the `WITH (...)` option-value shape.
TASK-237 landed the parallel-field AST shape (a dedicated `map` field
alongside the flat literal-valued options list); §8 pins the matching
parser surface and the error sites for the duplicate-map-clause and
empty-map-clause cases. The grammar productions in §8.2 are neutral to
the AST-layering decision — only `parse_with_clause`'s return tuple
reflects it.
