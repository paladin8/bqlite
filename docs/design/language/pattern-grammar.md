# BQL Pattern Grammar — Surface Syntax and AST Mapping

**Status:** accepted, Wave 3
**Task:** TASK-303
**Depends on:** docs/design/query-language.md §4, §6, §26; docs/design/sequence-matching.md §1-8
**Inputs (fixed):** `crates/bqlite-ast/src/pattern.rs`, `crates/bqlite-ast/src/operator.rs` (shipped by TASK-221 / TASK-224)
**Unblocks:** TASK-312 (parser pattern productions), TASK-313 (parser MATCH pipeline stage), TASK-316 (parser FUNNEL stage)

---

## 1. Purpose

This document is the normative mapping between BQL surface tokens and the
Wave 2-shipped AST for every production that involves the sequence pattern
grammar. TASK-312 and TASK-313 implement a recursive-descent parser against
this mapping — the grammar productions in §26 of `query-language.md` are the
canonical source of truth, and this document is the *how we implement §26*
layer for the pattern subsystem, parallel to `grammar-framework.md` for the
overall parser framework.

The document also performs a gap analysis (Section 7) identifying every case
where the shipped AST and the §26 grammar are not in one-to-one correspondence.
TASK-312 / TASK-313 implementors must read Section 7 before writing a line of
parser code.

---

## 2. Scope

Three BQL pipeline operators produce or consume the pattern grammar:

| Operator | AST type | Grammar production |
|----------|----------|--------------------|
| `MATCH`  | `PipelineStage::Match { pattern: MatchPattern, span }` | `match_op` |
| `FUNNEL` | `PipelineStage::Funnel(Funnel)` | `funnel_op` |
| `RETENTION` | `PipelineStage::Retention(Retention)` | `retention_op` |

MATCH and FUNNEL both share the `step_list` sub-grammar (→ `Vec<Step>`).
RETENTION uses a separate `retention_args` sub-grammar and does not use
`step_list`.

Variable bindings (`$var`) appearing inside step predicates are parsed as
`Expr::Variable` by the existing expression grammar (`expr.rs`). The pattern
module does not own variable binding parsing — only recognition that `$ident`
inside a `WHERE` clause is the binding reference form.

---

## 3. Grammar Productions and Token-to-AST Map

The §26 grammar is reproduced here for each relevant production, followed by
the exact AST mapping. For the full context-free grammar, see
`docs/design/query-language.md §26`.

### 3.1 `match_op` → `PipelineStage::Match`

```
match_op := MATCH match_mode SEQUENCE "(" step_list ")" match_modifiers
```

**Token-to-AST mapping:**

| Token / sub-production | Consumed by | AST target |
|------------------------|-------------|------------|
| `MATCH` keyword | `parse_match_op` | span anchor (start) |
| `match_mode` | `parse_match_mode` | `MatchPattern::mode` |
| `SEQUENCE` keyword | expect_kw | span |
| `"("` | expect_punct LParen | span |
| `step_list` | `parse_step_list` | `MatchPattern::steps: Vec<Step>` |
| `")"` | expect_punct RParen | span (end of step_list) |
| `match_modifiers` | `parse_match_modifiers` | `MatchPattern::window`, `MatchPattern::brackets`, + EMIT ALL flag (see §7.1) |

The production function lives in the new module `crates/bqlite-parser/src/match_op.rs`
registered in `pipeline::parse_stage` as the `Keyword::Match` arm.

**Span:** from the `MATCH` keyword token through the last token consumed by
`match_modifiers` (or the closing `)` of `SEQUENCE(...)` when no modifiers
are present).

**Error sites (grammar-framework.md §4.3):**

| Condition | Expected | detail |
|-----------|----------|--------|
| Missing `SEQUENCE` after match mode | `Expected::Keyword("SEQUENCE")` | `"MATCH requires SEQUENCE(...) — did you mean MATCH FIRST SEQUENCE(...) ?"` |
| Missing `(` after `SEQUENCE` | `Expected::Punct("(")` | `None` |
| Empty step list `SEQUENCE()` | `Expected::Keyword("step")` | `"SEQUENCE requires at least one step"` |
| Missing `)` after step list | `Expected::Punct(")")` | `None` |
| Out-of-order modifiers | see §3.6 | per-modifier message |

---

### 3.2 `match_mode` → `MatchMode`

```
match_mode := FIRST | ALL
```

| Token | AST value |
|-------|-----------|
| `FIRST` keyword | `MatchMode::First` |
| `ALL` keyword | `MatchMode::All` |

**Note on `MatchMode::EmitAll`:** This variant is **not** produced directly
from `match_mode`. The `EMIT ALL` modifier in `match_modifiers` interacts with
the parsed mode to produce the final value stored in `MatchPattern::mode`. See
§7.1 for the full encoding decision.

**Error site:**

| Condition | Expected | detail |
|-----------|----------|--------|
| Neither `FIRST` nor `ALL` after `MATCH` | `Expected::Keyword("FIRST or ALL")` | `"MATCH requires a mode: MATCH FIRST ... or MATCH ALL ..."` |

---

### 3.3 `step_list` → `Vec<Step>`

```
step_list := step (step_sep step)*
```

The step list is a non-empty sequence of steps with separators between them.
The separators are parsed inside `parse_step_list` as part of the loop —
each iteration of the loop parses one separator + one step and applies the
separator's properties to the **preceding** step.

**Algorithm for step_list parsing:**

```
1. Parse the first step → push to steps.
2. Loop:
   a. Peek. If the next token does NOT start a step_sep
      (i.e., it is NOT WITHOUT, THEN, or "->"), break the loop.
   b. Parse step_sep → captures {without: Option<Exclusion>, immediately: bool}.
   c. Parse the next step → push to steps.
   d. Apply the parsed step_sep to steps[len-2] (the step before the one
      just pushed):
      - step_sep.without → steps[len-2].without_next
      - step_sep.immediately → steps[len-2].immediately_next
3. Return steps.
```

The `without_next` and `immediately_next` fields on a `Step` refer to the
transition from **that step to the following step** — they are metadata about
the separator after the step, not before it. The parser therefore sets these
fields on `steps[i]` when it processes the separator between `steps[i]` and
`steps[i+1]`.

**Implementor note on `IMMEDIATELY` placement:** In source text, `IMMEDIATELY`
appears *after* `THEN` (e.g., `A THEN IMMEDIATELY B`), which may suggest it
belongs to step `B`. The AST stores it on the **preceding** step `A` as
`immediately_next: true`, meaning "the next step after me must immediately
follow me." Do not store it on step `B` — the field is `Step::immediately_next`
(gap *after* this step), not `Step::immediately_preceding` (gap *before* this
step). Both representations are semantically equivalent, but the shipped AST
uses the "gap after" convention.

The last step in the list never has `without_next` set (trailing WITHOUT is a
parse error — see §3.4 error sites). `immediately_next` on the last step is
also always `false`.

---

### 3.4 `step_sep` → applied to `Step`

```
step_sep := (WITHOUT exclusion)? (THEN | "->") IMMEDIATELY?
```

The step separator is not an AST node; its components are applied to the
preceding step's fields:

| Token / sub-production | Applied to | AST target |
|------------------------|------------|------------|
| `WITHOUT exclusion` (optional) | previous `Step` | `Step::without_next: Option<Exclusion>` |
| `THEN` keyword or `->` token | — | consumed; both are equivalent separators |
| `IMMEDIATELY` keyword (optional) | previous `Step` | `Step::immediately_next: bool` |

**Lookahead requirement:** The step_sep production uses up to 3-token lookahead
in one spot: after parsing `WITHOUT <event_ref>`, the parser needs to see
`THEN` (or `->`) to know the WITHOUT clause is complete and the separator
continues. This matches grammar-framework.md §7.4 which notes "Wave 3's MATCH
step separators push this to 3 in one spot (`WITHOUT <event_ref> THEN`)."

**`->` alias:** `Arrow` (`TokenKind::Arrow`) is lexed as a dedicated token
kind by the existing lexer (grammar-framework.md §6.2). The parser treats
`Arrow` identically to `Kw(Keyword::Then)` in the step separator position.

**Error sites:**

| Condition | Expected | detail |
|-----------|----------|--------|
| Missing `THEN` or `->` after `WITHOUT <exclusion>` | `Expected::Keyword("THEN")` | `"step separator THEN or -> expected between steps"` |
| `WITHOUT` at end of step list (trailing WITHOUT) | `Expected::EventRef` | `"WITHOUT must appear between two steps"` |
| Duplicate `IMMEDIATELY` in one separator | `Expected::step` | `"IMMEDIATELY appears twice in one step separator"` |

---

### 3.5 `step` → `Step`

```
step := unqualified_step repetition?
      | "(" step ")" repetition?     -- parenthesized group (required for WHERE + repetition)
```

The parenthesized form `"(" step ")" repetition?` exists solely to
disambiguate `WHERE predicate +` from expression-level `+` inside the
predicate (query-language.md §4.9). The parser strips the parentheses and
produces a **flat `Step`** — the parenthesized form does not survive into the
AST. The `repetition` suffix on the outer production is applied to the inner
step before returning:

```
parse_step():
  if peek == LParen AND next_token_is_OR_or_step_group():
    # The ( opens a parenthesized step group (WHERE + repetition).
    # Alternation (a OR b) is handled by parse_step_event inside
    # parse_unqualified_step — both paths call parse_unqualified_step.
    bump()                   # consume "("
    inner = parse_unqualified_step()
    expect RParen
    inner.repetition = parse_repetition()   # applies to the parenthesized group
    inner.span = merged(lparen_span, rparen_span, repetition_span)
    return inner
  else:
    # Bare event ref (or alternation — parse_step_event handles both)
    s = parse_unqualified_step()
    s.repetition = parse_repetition()
    return s
```

`next_token_is_OR_or_step_group()` is the lookahead described in §3.7. In
practice, TASK-312 should implement this as: look past the `(` (at `peek_at(1)`
and `peek_at(2)` or `peek_at(3)` for qualified refs) to find `OR`. If `OR` is
found before `)`, it is an alternation — call `parse_unqualified_step` which
will call `parse_step_event` which will re-encounter and handle the `(` as
alternation syntax. If `OR` is not found, it is a parenthesized step group —
consume the `(`, call `parse_unqualified_step`, then expect `)`.

**Note:** both branches call `parse_unqualified_step`. The alternation case does
NOT consume the `(` before calling it (so `parse_step_event` inside sees the
`(` and handles it as alternation). The parenthesized-step-group case DOES
consume the `(` before calling `parse_unqualified_step` (so the inner call
sees a bare step without surrounding parens).

**Recursive parenthesized steps:** The grammar writes the right-hand branch as
`"(" step ")"` (recursive), but in practice only one level of nesting is
meaningful — a `WHERE`-bearing step grouped for repetition. TASK-312 may choose
to parse it flat (`unqualified_step`) rather than recursively, since the nested
step inside parens cannot itself carry an outer repetition suffix that conflicts
with the inner WHERE. Implementing flat `unqualified_step` inside parens is
simpler and produces identical AST output.

---

### 3.6 `unqualified_step` → `Step` (inner fields)

```
unqualified_step := (identifier ":")? step_event (WHERE predicate)?
```

| Token / sub-production | AST target |
|------------------------|------------|
| `identifier ":"` (optional) | `Step::name: Option<Name>` |
| `step_event` | `Step::event: StepEvent` |
| `WHERE predicate` (optional) | `Step::predicate: Option<Spanned<Expr>>` |

**Step name parsing:**
The optional `identifier ":"` prefix is disambiguated by 2-token lookahead:
peek at token[0] (must be `Ident`) and token[1] (must be `Colon`). If both
match, consume both and store `Name { text: <ident>, span }` in `Step::name`.
If the lookahead fails (token[1] is not `Colon`), no name is consumed and
`Step::name = None`.

**Reserved keywords as step names:** Step names are bare `identifier` tokens.
The §26.3 rule applies: a reserved keyword used as a step name without
backtick quoting is a `ParseError::ReservedKeyword` error. In practice, most
event names used as steps are not reserved (they are lowercase user-defined
names), but the parser must check.

**Span:** from the leading `identifier ":"` (or `step_event` start if no name)
through the end of the `WHERE predicate` (or end of `step_event` if no WHERE).

---

### 3.7 `step_event` → `StepEvent`

```
step_event := event_ref
            | "(" event_ref (OR event_ref)+ ")"
```

| Form | AST value |
|------|-----------|
| bare `event_ref` | `StepEvent::Single(EventRef)` |
| `"(" event_ref (OR event_ref)+ ")"` | `StepEvent::Alternation(Vec<EventRef>)` |

The alternation form requires at least two `event_ref` entries (the grammar
writes `event_ref (OR event_ref)+`, not `event_ref (OR event_ref)*`). A
`( single_event )` with no `OR` is a parse error at the step-event level.

**Why `(single_event)` is rejected in `step_event` but accepted in `exclusion`
(§3.9):** At the `step` level, `(` is already the disambiguation token for a
parenthesized step group (§3.5) — a `(` followed by a name that is not
followed by `OR` is consumed as a parenthesized `unqualified_step`. By the
time `parse_step_event` is called, the outer `(` has either been consumed
(parenthesized step group) or not (bare event ref). The `(` that
`parse_step_event` sees always starts the alternation form — a single-event
`(event)` would be meaningless and ambiguous with a parenthesized step group,
so it is rejected. In contrast, `exclusion` has no parenthesized-group
ambiguity (exclusions are not steps), so `WITHOUT (single_event)` is
accepted as a single-element exclusion vec.

(The outer repetition parenthesization from §3.5 would have already consumed
the `(` before `parse_step_event` is called.)

**Disambiguation of `"("` at the step level:**
When `parse_step` sees `LParen`, it must decide whether the `(` opens:
- A parenthesized step group (for repetition + WHERE) — calls `parse_unqualified_step` inside.
- A step-event alternation — the alternation `( event OR event ... )` is parsed
  within `parse_unqualified_step` via `parse_step_event`.

The disambiguation rule: after `(`, peek ahead to find `OR`:
- `peek_at(1) == OR` → alternation on an unqualified event ref.
- `peek_at(1) == ")"` → single-element parens (parse error — see §3.7 note).
- `peek_at(1) == "."` → qualified event ref (`table.event`); peek further:
  `peek_at(3) == OR` → alternation; otherwise → parenthesized step group.
- Anything else → parenthesized step group (call `parse_unqualified_step`).

In practice the lookahead depth is at most 4 (for `(table.event OR ...)`)
which is within the `peek_at(n)` contract from grammar-framework.md §7.4.

Note: the alternation case inside `parse_step` is handled by forwarding to
`parse_unqualified_step`, which calls `parse_step_event`. The `parse_step`
function does not handle the `(OR)` tokens directly — it either unwraps the
outer `()` for a parenthesized step group, or falls through to `parse_unqualified_step`
which handles both bare and alternation `step_event`.

---

### 3.8 `event_ref` → `EventRef`

```
event_ref := (name ".")? name    -- table.event_type in multi-table queries
```

| Token | AST target |
|-------|------------|
| optional `name "."` prefix | `EventRef::table: Option<Name>` |
| final `name` | `EventRef::event: Name` |

**Table-qualified form:** `events.signup` produces `EventRef { table: Some("events"), event: "signup" }`. For single-table queries the parser emits `table: None` when no dot-qualified prefix appears.

**Disambiguation of `.`:** The `name "."` prefix is consumed only when a `Dot`
token immediately follows the first `name` token. The parser peeks 1 ahead: if
`peek_kind() == Dot`, it bumps the name and dot and then expects another name.
Otherwise it returns the first name as the event with `table: None`.

**Span:** from start of the first `name` (or the table `name` if qualified)
through the end of the final `name` token.

---

### 3.9 `exclusion` → `Exclusion`

```
exclusion := event_ref
           | "(" event_ref (OR event_ref)+ ")"
```

The exclusion grammar mirrors `step_event` exactly. The mapping is:

| Form | AST |
|------|-----|
| bare `event_ref` | `Exclusion { events: vec![event_ref], span }` |
| `"(" event_ref (OR event_ref)+ ")"` | `Exclusion { events: vec![...], span }` |

Both forms produce `Exclusion::events: Vec<EventRef>`. A bare `event_ref`
produces a single-element vec; the parenthesized form produces multiple
elements.

**Error site:**

| Condition | Expected | detail |
|-----------|----------|--------|
| Empty `WITHOUT ()` | `Expected::EventRef` | `"WITHOUT requires at least one event type"` |
| `WITHOUT (single_event)` (no OR, one element) | — | accepted (single exclusion in parens; vector has one element) |

---

### 3.10 `repetition` → `Repetition`

```
repetition := "*" | "+"
```

| Token | AST value |
|-------|-----------|
| `Star` (`*`) | `Repetition::ZeroOrMore` |
| `Plus` (`+`) | `Repetition::OneOrMore` |

Repetition is optional — `try_kind(Star)` / `try_kind(Plus)` returns
`Option<Token>`. When absent, `Step::repetition = None`.

**Constraint:** A step carrying both `where predicate` and `repetition` must be
parenthesized (§3.5). The parser does not enforce this constraint at the step
level (it only parses what it sees) — the planner enforces it via the grammar
ambiguity rule documented in query-language.md §4.9.

---

### 3.11 `match_modifiers` → `MatchPattern` fields

```
match_modifiers := (WITHIN (duration | SESSION))?
                   (BRACKETS CUMULATIVE? "[" duration_list "]")?
                   (EMIT ALL)?
duration_list   := duration ("," duration)*
```

The modifiers are optional and **order-constrained** (WITHIN/BRACKETS before
EMIT ALL). The parser enforces order by attempting each optional production in
canonical sequence and returning an error if a modifier appears out of order.

#### 3.11.1 `WITHIN` clause → `MatchPattern::window`

| Token sequence | AST value |
|----------------|-----------|
| `WITHIN duration` | `Some(MatchWindow::Within(nanos: i64))` |
| `WITHIN SESSION` | `Some(MatchWindow::WithinSession)` |
| (absent) | `None` |

`duration` is a `TokenKind::Duration(i64)` fused lexer token (grammar-framework.md §6.4). The nanosecond value is stored directly — no conversion needed.

`WITHIN SESSION` requires 2-token lookahead: after `WITHIN`, peek for `SESSION`
keyword. If present, consume it and emit `WithinSession`. Otherwise emit
`Within(duration)` and parse the `duration` token.

**WITHIN and BRACKETS are mutually exclusive.** Per §26.1 grammar notes,
the mutual-exclusivity check is the **planner's** responsibility, not the
parser's. The parser enforces only the canonical order (WITHIN/BRACKETS before
EMIT ALL) — it does not reject a query that specifies both WITHIN and BRACKETS
in the correct order. The planner raises a typed error when both fields are
non-None on the parsed `MatchPattern`.

#### 3.11.2 `BRACKETS` clause → `MatchPattern::brackets`

| Token sequence | AST value |
|----------------|-----------|
| `BRACKETS "[" duration_list "]"` | `Some(BracketSpec { durations, cumulative: false, span })` |
| `BRACKETS CUMULATIVE "[" duration_list "]"` | `Some(BracketSpec { durations, cumulative: true, span })` |
| (absent) | `None` |

`duration_list` is parsed as one or more `Duration` tokens separated by
`Comma`. An empty bracket list `BRACKETS []` is a parse error
(`detail: "BRACKETS requires at least one duration"`).

**Bracket duration ordering:** The parser stores durations in user-declared
order without sorting. The planner validates that brackets are strictly
increasing.

#### 3.11.3 `EMIT ALL` → `MatchMode` encoding (see §7.1)

`EMIT ALL` is two tokens: `Kw(Emit)` followed by `Kw(All)`. After parsing
`match_mode` and `step_list` and the optional WITHIN/BRACKETS, the parser
calls `try_kw(Emit)`. If present, it expects `Kw(All)` and records that
EMIT ALL was requested.

**How EMIT ALL modifies `MatchPattern::mode`:** See Section 7.1 for the full
encoding decision. Summary: `FIRST + EMIT ALL` → `MatchMode::EmitAll`;
`ALL + EMIT ALL` → **parse error in v1** (AST cannot represent it).

**Error site:**

| Condition | Expected | detail |
|-----------|----------|--------|
| `EMIT` not followed by `ALL` | `Expected::Keyword("ALL")` | `"expected ALL after EMIT"` |
| Out-of-order modifier (`EMIT ALL WITHIN ...`) | `Expected::EndOfModifiers` | `"WITHIN must appear before EMIT ALL"` |
| Out-of-order modifier (`BRACKETS WITHIN ...`) | `Expected::EndOfModifiers` | `"WITHIN must appear before BRACKETS"` |

---

### 3.12 `funnel_op` → `PipelineStage::Funnel`

```
funnel_op        := FUNNEL "(" step_list ")" funnel_modifiers
funnel_modifiers := (WITHIN duration)?
```

| Token / sub-production | AST target |
|------------------------|------------|
| `FUNNEL` keyword | span anchor |
| `"("` | span |
| `step_list` | `Funnel::steps: Vec<Step>` |
| `")"` | span |
| `WITHIN duration` (optional) | `Funnel::window: Option<i64>` (nanoseconds) |

`Funnel::window` stores the raw nanosecond value from the `Duration` token, or
`None` if the `WITHIN` clause is absent. Unlike `MatchPattern::window` which is
`Option<MatchWindow>`, the `Funnel::window` field is `Option<i64>` — the AST
discards the `Within` wrapper because FUNNEL does not support `WITHIN SESSION`.

The funnel DOES NOT accept BRACKETS or EMIT ALL in its surface form (those are
generated by the planner during desugaring). If the parser sees either keyword
after the closing `)`, they belong to a subsequent pipeline stage, not the
FUNNEL clause.

**Error sites:** Mirror `match_op` error sites for the `step_list`. Add:

| Condition | Expected | detail |
|-----------|----------|--------|
| `EMIT` after FUNNEL | `Expected::Pipe or Eof` | `"FUNNEL does not accept EMIT ALL — use MATCH FIRST ... EMIT ALL instead"` |
| `BRACKETS` after FUNNEL | `Expected::Pipe or Eof` | `"FUNNEL does not accept BRACKETS — use MATCH FIRST ... BRACKETS [...] instead"` |

---

### 3.13 `retention_op` → `PipelineStage::Retention`

```
retention_op   := RETENTION "(" retention_args ")"
retention_args := "entry" ":" event_ref "," "activity" ":" event_ref ","
                  "brackets" ":" "[" duration_list "]"
                  ("," "cumulative" ":" bool_lit)?
```

The RETENTION stage uses keyword-parameter syntax (similar to SESSIONIZE and
ATTRIBUTE) rather than the positional step grammar.

| Token sequence | AST target |
|----------------|------------|
| `"entry" ":" event_ref` | `Retention::entry: EventRef` |
| `"activity" ":" event_ref` | `Retention::activity: EventRef` |
| `"brackets" ":" "[" duration_list "]"` | `Retention::brackets.durations` |
| `"cumulative" ":" bool_lit` (optional) | `Retention::brackets.cumulative` |

Parameter keys (`entry`, `activity`, `brackets`, `cumulative`) are plain
`Ident` tokens, not reserved keywords. They are matched case-sensitively
(consistent with query-language.md §26.3 identifier resolution).

The `bool_lit` for `cumulative` is `TRUE` or `FALSE` (keyword tokens). If
`cumulative` is absent, `BracketSpec::cumulative = false`.

**Argument order enforcement:** The grammar production in §26 lists arguments
in fixed order (`entry`, `activity`, `brackets`, then optional `cumulative`).
The parser **enforces this order** — it expects each key in sequence and emits
a specific error if a key appears out of order. It does not accept key/value
pairs in arbitrary order. The rationale: fixed-order parsing gives better error
messages ("expected 'activity' key but found 'brackets'") than a collect-then-validate
approach, and the grammar is unambiguous in its ordering.

Trailing comma before `)` is **not** permitted in `retention_args` (unlike
`WITH (...)` option lists which do accept trailing commas). The grammar does
not specify it, so the parser does not accept it.

**Error sites:**

| Condition | Expected | detail |
|-----------|----------|--------|
| Key out of order (e.g., `activity` before `entry`) | `Expected::Identifier("entry")` | `"RETENTION parameters must appear in order: entry, activity, brackets, cumulative"` |
| Unknown key in retention args | `Expected::Identifier` | `"unknown RETENTION parameter; expected entry, activity, brackets, or cumulative"` |
| Duplicate key | `Expected::Comma or RParen` | `"duplicate RETENTION parameter: <key>"` |
| Missing required key (`entry`, `activity`, `brackets`) | `Expected::Identifier` | `"RETENTION requires entry, activity, and brackets parameters"` |

---

## 4. Named Step Auto-Naming

The AST stores `Step::name = None` when the user does not provide a step name.
**Auto-naming is the planner's responsibility**, not the parser's. Per
query-language.md §4.4:

- Steps without explicit names get auto-generated names `step_0`, `step_1`, …
  (0-indexed by position in the step list).
- Repeated event types without names get numeric suffixes: `page_view_0`,
  `page_view_1`.

The parser stores `None` and the planner fills in synthetic names during
logical lowering. TASK-312 does not generate auto-names.

---

## 5. Variable Bindings in Step Predicates

Variable references (`$plan`, `$price`) inside `WHERE` predicates are parsed
by the existing expression grammar as `Expr::Variable(Name)` (the `$`
character is already handled by `TokenKind::Variable(String)` in the lexer —
grammar-framework.md §6.2 "Variable(String) — `$foo` → text `foo`").

The parser does not track which variables are bound vs. referenced — that is
the planner's job. The step predicate is an arbitrary `Spanned<Expr>` that
may contain zero or more `Expr::Variable` sub-expressions.

**Scope:** Variable names are case-sensitive (`$Plan` and `$plan` are
different variables). They are scoped to a single MATCH expression at plan
time but the parser does not enforce scoping.

---

## 6. Parenthesized Repetition Groups

The grammar requires parentheses around any step that combines a `WHERE`
clause with a repetition suffix:

```bql
-- REQUIRED: parentheses around WHERE + repetition
MATCH FIRST SEQUENCE(signup THEN (page_view WHERE category = 'shop')+ THEN purchase)

-- REJECTED: ambiguous (the + could be arithmetic in the predicate)
MATCH FIRST SEQUENCE(signup THEN page_view WHERE category = 'shop' + THEN purchase)
```

The parser accepts the parenthesized form per §3.5. The bare form `event_ref WHERE
predicate repetition` is not in the grammar and the parser does not special-case
it — the `+` or `*` after a WHERE clause is simply not consumed at the step
level, causing a parse error at the step separator position.

The parser does **not** need to detect "you probably forgot parentheses" — the
error will manifest as "expected THEN or -> after step" when the expression
parser has consumed the `+` as part of the predicate, which is a reasonable
diagnostic.

---

## 7. Gap Analysis

This section documents every place where the shipped AST (`pattern.rs`,
`operator.rs`) and the §26 grammar are not in perfect one-to-one
correspondence. Each gap must be resolved (either by amending the AST or by
constraining the grammar surface) before TASK-312 can produce a complete parser.

### 7.1 `MatchMode::EmitAll` Encoding — Primary Gap

**Issue:** The grammar has `match_mode × EMIT ALL` as orthogonal: `FIRST`, `ALL`,
`FIRST EMIT ALL`, and `ALL EMIT ALL` are all syntactically valid. The shipped
AST `MatchMode` has three variants: `First`, `All`, `EmitAll`. There is no
representation for `ALL + EMIT ALL`.

```rust
pub enum MatchMode {
    First,    // MATCH FIRST
    All,      // MATCH ALL
    EmitAll,  // MATCH FIRST EMIT ALL (funnel desugaring)
              // MATCH ALL EMIT ALL — CANNOT BE REPRESENTED
}
```

**Root cause:** `MatchMode::EmitAll` was shipped as the representation for
"FUNNEL desugaring" which always uses `FIRST + EMIT ALL`. The `ALL + EMIT ALL`
combination was not considered when the AST was designed.

**Resolution for TASK-312/313:** Until the AST is amended, adopt this encoding:

| Surface form | `MatchPattern::mode` |
|---|---|
| `MATCH FIRST` | `MatchMode::First` |
| `MATCH ALL` | `MatchMode::All` |
| `MATCH FIRST ... EMIT ALL` | `MatchMode::EmitAll` |
| `MATCH ALL ... EMIT ALL` | **parse error** — `"MATCH ALL EMIT ALL is not supported in this version; use MATCH FIRST EMIT ALL or MATCH ALL without EMIT ALL"` |

This restriction is conservative and covers all known Wave 3 use cases:
- The Wave 3 acceptance test uses `MATCH FIRST ... WITHIN 7d` (no EMIT ALL).
- FUNNEL desugaring generates `MATCH FIRST ... EMIT ALL` → `MatchMode::EmitAll`.
- `MATCH ALL EMIT ALL` is not required by any Wave 3 task.

**Recommended AST amendment (future task):** Add `emit_all: bool` to
`MatchPattern` and reduce `MatchMode` to `First | All`. `MatchMode::EmitAll`
becomes `MatchMode::First` with `emit_all: true`. The funnel desugaring path
updates to set `emit_all: true` explicitly. File as a Wave 4 cleanup task
when `ALL + EMIT ALL` is needed by a real query.

### 7.2 `Step::immediately_next` Placement

**Issue:** `Step::immediately_next` represents the transition constraint
"IMMEDIATELY" which appears in the separator **after** the current step but
is stored **on** the current step (as a field describing the gap to the next step).

**Clarification (not a gap):** This is intentional and correct. The last step
in the list always has `immediately_next = false` because there is no following
step. The parser applies IMMEDIATELY to `steps[i]` when processing the separator
between `steps[i]` and `steps[i+1]`, as documented in §3.4. TASK-312 must
follow this convention.

### 7.3 `Step::without_next` — Single vs. Multiple Events

**Issue:** `Exclusion::events: Vec<EventRef>` supports multiple exclusion
events (`WITHOUT (refund OR churn)`). The surface grammar's bare form
(`WITHOUT event_ref`) produces `Exclusion { events: vec![event_ref] }` — a
single-element vec. The AST allows zero-element vecs, which the parser must
never produce (an empty `Exclusion` is a planner error, not a parser error).

**Clarification:** No AST change needed. The parser always produces
`events.len() >= 1`. See §3.9 error sites.

### 7.4 `MatchMode` and `PipelineStage::Match` vs. `Funnel`

**Note:** `PipelineStage::Funnel(Funnel)` uses `Vec<Step>` directly (no
`MatchPattern` wrapper). The `Funnel::window` field is `Option<i64>` (raw
nanoseconds), not `Option<MatchWindow>`. This is a deliberate AST design
choice — FUNNEL does not support `WITHIN SESSION`, so the `MatchWindow` enum
variant is not needed.

This creates a minor redundancy: two places in the AST store duration-as-nanos
(one as `MatchWindow::Within(i64)`, one as `Funnel::window: Option<i64>`).
This is acceptable for v1 — the planner's FUNNEL desugaring translates
`Funnel::window` to a `MatchPattern` with `MatchWindow::Within(nanos)`.

### 7.5 `Funnel::window` — Missing `WithinSession`

**Issue:** `Funnel::window: Option<i64>` cannot represent `WITHIN SESSION`.
The grammar does not list `SESSION` as valid inside `funnel_modifiers` either:

```
funnel_modifiers := (WITHIN duration)?
```

**Not a gap:** `WITHIN SESSION` is correctly excluded from FUNNEL sugar — a
FUNNEL inside a SESSIONIZE context is handled by writing `MATCH FIRST
SEQUENCE(...) WITHIN SESSION EMIT ALL` explicitly. This is intentional per
query-language.md §6.

### 7.6 `Retention::brackets` — Always Non-Cumulative Default

**Issue:** `BracketSpec` has a `cumulative` field, but the RETENTION grammar
requires the `cumulative` key to be explicit:

```
("," "cumulative" ":" bool_lit)?
```

**Clarification:** When the `cumulative` key is absent, `BracketSpec::cumulative
= false`. When present with `TRUE`, `cumulative = true`. When present with
`FALSE`, `cumulative = false` (redundant but valid). No gap.

### 7.7 Missing `SORT` Alias for `ORDER BY`

**Note:** This gap belongs to TASK-315, not to the pattern grammar. Mentioned
here for completeness since it appears adjacent to pattern-related pipeline
stages in TASKS.md. Pattern grammar does not include `SORT`.

### 7.8 `Step::span` — Derivation

The `Step::span` should cover the full extent of the step from its leading
token (name identifier if named, or first token of `step_event`) through the
end of the `WHERE predicate` (or repetition suffix if present). Grammar-
framework.md §5 span conventions apply: start = first token's start byte,
end = last consumed token's end byte.

For the parenthesized step form, the span should include the enclosing
parentheses: from `(` to `)` and through the trailing repetition token if any.

---

## 8. Implementation Notes for TASK-312 and TASK-313

### 8.1 Module Layout

Per grammar-framework.md §3, Wave 3 adds:

```
crates/bqlite-parser/src/
├── match_op.rs     # match_op, step_list, step, step_sep, step_event,
│                   # event_ref, exclusion, repetition, match_modifiers
│                   # (TASK-312 primary output)
├── funnel.rs       # funnel_op  (TASK-316)
```

TASK-313 extends `pipeline.rs` to add the `Keyword::Match` arm in
`parse_stage` — the arm calls `match_op::parse_match_op(p)`.

TASK-316 extends `pipeline.rs` to add the `Keyword::Funnel` arm — the arm
calls `funnel::parse_funnel_op(p)`. RETENTION is also a pipeline arm but
is deferred to a later task.

### 8.2 Helper Reuse

The `parse_event_ref` function is shared between `match_op.rs` and
`funnel.rs`. It should live on `impl Parser<'s>` (or as a `pub(crate)` free
function in `match_op.rs` imported by `funnel.rs`) since it is needed by
at least two production modules. Grammar-framework.md §7.1 pattern: shared
helpers live on `Parser<'s>` or in a module imported by both.

Similarly `parse_step_list`, `parse_step`, and `parse_step_event` are shared
between MATCH and FUNNEL. They should be defined in `match_op.rs` and called
by `funnel.rs`.

### 8.3 `parse_duration_list`

A helper to parse `"[" duration ("," duration)* "]"` is needed by both
`match_modifiers` (BRACKETS) and `retention_op` (brackets parameter). It
should live on `Parser<'s>` for reuse by the eventual RETENTION parser.

### 8.4 Keyword Disambiguation Inside Step Predicates

Inside a `WHERE predicate`, the expression parser may encounter:
- `AND`, `OR`, `NOT`, `IS`, `IN`, `BETWEEN`, `LIKE` — absorbed by the expr
  grammar (no issue).
- `THEN`, `WITHOUT`, `IMMEDIATELY`, `EMIT`, `WITHIN`, `BRACKETS` — these
  terminate the predicate and are consumed by the step/modifier parser.

The Pratt parser in `expr.rs` must not consume these keywords as binary
operators. Since they are not in the Pratt operator table, they will cause
`parse_expr_bp` to stop and return the accumulated expression — this is the
correct behavior. No special handling is needed.

### 8.5 `WITHIN SESSION` Two-Token Lookahead

After consuming `WITHIN`, peek at the next token:
- If `Kw(Session)` → consume it and return `MatchWindow::WithinSession`.
- Otherwise → parse `Duration` token and return `MatchWindow::Within(nanos)`.

Do not consume `SESSION` eagerly — it may be a column name in an expression
context (`WHERE session = 'foo'`). The two-token lookahead is safe because the
parser is at the modifier position (after `SEQUENCE(...)`), not inside an
expression.

### 8.6 Minimum Test Requirements (grammar-framework.md §9)

Per the framework's §9.1 "three tests per production, minimum":

**`match_op`:** happy-path 3-step funnel, error on missing SEQUENCE, span
preservation from MATCH keyword to last modifier.

**`step_list`:** 1-step list, multi-step list with THEN and `->` aliases mixed,
IMMEDIATELY modifier propagated to preceding step, WITHOUT exclusion on a step.

**`step` / `step_event`:** bare event, alternation `(a OR b)`, parenthesized
repetition `(a WHERE x = 1)+`, error on `a WHERE x = 1+` (missing parens).

**`match_modifiers`:** WITHIN only, BRACKETS only, EMIT ALL only, all three
together, error on reversed order, error on `ALL + EMIT ALL`.

**`funnel_op`:** happy-path, WITHIN modifer present, WITHIN absent.

For the round-trip proptest (grammar-framework.md §9.2), add a
`arb_match_pattern()` strategy to `tests/src/strategies.rs` that generates
`MatchPattern` values that can be printed and re-parsed. Limit the generated
patterns to avoid combinatorial explosion: cap step count at 4, no nested
alternation inside WITHOUT, only `Within(nanos)` for the window (not
`WithinSession` — because `WITHIN SESSION` requires an upstream `SESSIONIZE`
stage to be semantically valid, and a round-trip fixture without that context
cannot be executed end-to-end), and no BRACKETS (the planner validates strict
ascending bracket order which the random generator would need to enforce).
The invariant: `parse(print(pattern)) == pattern` (up to spans).

---

## 9. Cross-Reference Map

| Feature | Query Language §| Sequence Matching §| AST type |
|---------|----------------|---------------------|----------|
| MATCH modes | §4.2 | §5 | `MatchMode` |
| EMIT ALL | §4.3 | §5.3 | See §7.1 of this doc |
| Named steps | §4.4 | — | `Step::name` |
| Property predicates | §4.5 | §9 | `Step::predicate` |
| WITHIN | §4.6 | §2.2, §4 | `MatchWindow::Within` |
| WITHOUT negation | §4.7 | §3.4 | `Step::without_next`, `Exclusion` |
| Alternation | §4.8 | §3.3 | `StepEvent::Alternation` |
| Repetition | §4.9 | §3.5 | `Step::repetition`, `Repetition` |
| IMMEDIATELY | §4.10 | §4.4 | `Step::immediately_next` |
| Variable bindings | §4.11 | §8 | `Expr::Variable` in predicates |
| BRACKETS | §4.12 | — | `BracketSpec` |
| FUNNEL sugar | §6.1 | — | `Funnel` |
| RETENTION sugar | §6.3 | — | `Retention` |
| Formal grammar | §26 | — | (this document) |
