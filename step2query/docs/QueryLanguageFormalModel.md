# **The Formal Model of the STEP2 Query Language**

## Overview

The STEP2 query language operates over a database engine that supports:

- **Relational tables** with primary keys  
- **Foreign keys** (directed FK → PK edges)  
- **Sets** (owner → members postings lists stored per record)  
- **String dictionaries** (enabling fast equality and LIKE/prefix search)

These structural primitives define a **typed, directed, multi‑edge graph** over relational data.  
The query language is a **linear pipeline algebra** over this graph, composed of unary operators.

---

## Core Data Model

### Tables
Each table defines:

- A set of named attributes  
- A primary key  
- Optional foreign keys to other tables  
- Optional sets (postings lists) referencing records in other tables  

### Foreign Keys
A foreign key defines a **directed, functional edge**:

```
A.fk → B.pk
```

This supports **FK navigation**.

### Sets
A set defines an **owner → members** relationship:

```
A.S → B
```

Where `A.S` is a postings list stored in each record of `A`.  
This supports **Set navigation**.

### String Dictionaries
String attributes are indexed by a dictionary structure supporting:

- equality  
- prefix search (LIKE patterns)  

This enables efficient filtering in `where` stages.

---

## Query Model

A STEP2 query is a **pipeline**:

```
StartTable | Stage1 | Stage2 | … | StageN
```

Each stage is a **unary operator**:

**f : R → R′**

The pipeline evaluates as:

⟦Q⟧ = the result of applying the stages f₁, f₂, …, fₙ in order to the starting table, where each stage transforms the output of the previous one for the input of the next stage, or the query result. 

---

## Stage Types

### 1. Selection Stage
```
where BoolExpr
```
A pure filter:

**σᵩ(R)** = the set of all tuples t in R for which the predicate ϕ(t) evaluates to true.

Does not change schema.

---

### 2. Projection Stage
```
return A, B, C
```

A pure projection:

**π₍A,B,C₎(R)** = the set of tuples obtained by taking each tuple t in R and keeping only the attributes A, B, and C.

Defines a new schema.

---

### 3. FK Navigation Stage
```
navigate T on A.x == T.y return …
```

A **curried join + projection** operator:

**NavFK₍T, θ, π₎(R)** = the projection `π` applied to the join of the input relation R with table T under the join condition θ.


Where:

- `θ` is the FK join condition  
- `π` is the explicit return list  

This operator always ends with a projection.

---

### 4. Set Navigation Stage
```
navigate set A.S return …
```

A **postings‑list expansion + projection** operator:

**NavSet₍S,π₎(R)** =  the projection `π` applied to the set of all pairs (t, b) such that:

Where:
  
- t is a tuple in R, and 
- `S(t)` is the postings list attached to record `t`
- b is an element of the postings list `S(t)`
- `π` is the explicit property return list 

This operator always ends with a projection.

---

## Structural Properties of the Language

1. **Linear**  
   No branching, no subqueries, no backward navigation.

2. **Unary**  
   Every stage transforms one relation into another.

3. **Schema‑carrying**  
   Only `return` clauses define the schema for the next stage input.

4. **Navigation is directional**  
   FK navigation follows FK → PK.  
   Set navigation follows owner → members.

5. **Navigation stages include projection**  
   They cannot leak attributes forward unless explicitly returned.

6. **Final stage must be a projection**  
   Either an explicit `return` or the projection inside a navigation stage.

---

## Summary

STEP2 is a **typed, linear, unary navigational algebra** over a relational‑graph hybrid.  
Its primitive operators are:

- `σ` — selection  
- `π` — projection  
- `NavFK` — FK expand + projection  
- `NavSet` — Set expand + projection  

The pipeline operator `|` is function composition.

This algebra is:

- more expressive than classical relational algebra (due to sets)  
- more structured than graph query languages (due to typing and projection boundaries)  
- simpler and more predictable than CODASYL (sets are runtime, not schema‑declared)  
- easier to optimize due to strict linearity and explicit schema control  

---