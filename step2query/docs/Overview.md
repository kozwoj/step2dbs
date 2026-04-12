# Graph-Relational Pipeline Query Model

This document provides the conceptual overview of step2query. The goal of the project is to define a query language for STEP2 databases based on a directed, selective, schema-transforming pipeline.

The STEP2 data model is relational, but it adds one important navigation feature beyond ordinary foreign keys: a table record can carry a SET of references to records in another table. step2query is designed to navigate both:

- foreign-key to primary-key edges
- SET edges stored with source-table records

The language is intentionally narrower than SQL. It is designed around predictable forward movement through a schema graph instead of unrestricted relational composition.

## Design Goals

step2query supports:

- forward-only navigation along foreign-key to primary-key edges or SET edges
- selective projection at each navigation step
- evolving working record types across the pipeline
- explicit control over which attributes survive to the next stage
- atomicity of the stages, which all result in a collection of new record types

step2query does not support:

- global joins
- backward references
- implicit schema merging

The model is conceptually similar to a Kusto-style pipeline, but it adds a dedicated `navigate` operator for directed expansion of the current working record type.

## Core Concepts

### Working Record Type

A query starts with a source DB table. Each pipeline stage consumes the current working record type and produces either:

- the same schema, when the stage is only filtering rows
- a new in-memory schema, when the stage projects a new result shape

Only attributes explicitly returned by the current stage are carried into the next stage.

### Stage Types

The basic stage types are:

- `where`: filters the current working relation without changing its schema
- `navigate`: follows an FK to PK edge or a SET edge and combines source and target data
- `return`: projects the fields that define the next working record type

### Where

`where` applies selection to the current working relation (current set of records).

```text
| where Orders.Amount > 1000
```

It filters rows only. It does not change the shape of the current record type.

### Navigate

`navigate` is the central operator in the language. It performs directed expansion from the current working relation to a target relation.

Example using FK to PK navigation:

```text
Customers
| where Customers.Status == "Active"
| navigate Orders on Customers.CustomerID == Orders.CustomerID
      return Customers.CustomerName,
             Orders.OrderID,
             Orders.Amount
```

Example using SET navigation:

```text
Classes
| where Classes.Class_Code == "MATH101"
| navigate set Classes.Enrollment
      return Students.StudentID,
             Students.Advisor
```

`navigate`:

- follows a foreign-key to primary-key edge or a SET edge
- combines source records with matching target records
- produces a new record type defined by `return` projection
- drops attributes not listed in the projection

This is not a general-purpose relational join. It is a constrained, directional expansion step.

### Return

`return` defines exactly which fully qualified attributes survive to the next stage.

```text
return Customers.CustomerName,
       Orders.OrderID,
       Orders.Amount
```

Return items should stay fully qualified to avoid ambiguity and to make schema evolution across stages explicit.

## SET Navigation Constraint

SET navigation can only be the first navigation step in a pipeline.

This restriction follows from the STEP2 storage model: SET postings are associated with stored table records. After the first navigation produces an in-memory result, that direct association with the original postings list is no longer available. Foreign-key values, by contrast, remain materialized in the projected rows and can still support later FK to PK navigation.

## Example Pipelines

Example from a NorthWind-style schema:

```text
Customers
| where Customers.Status == "Active"
| navigate Orders on Customers.CustomerID == Orders.CustomerID
      return Customers.CustomerName,
             Orders.OrderID,
             Orders.Amount
| where Orders.Amount > 1000
| navigate Products on Orders.ProductID == Products.ProductID
      return Customers.CustomerName,
             Orders.OrderID,
             Products.ProductName,
             Products.Price
| where Products.Price > 50 
| return Customers.CustomerName, 
      Orders.OrderID, 
      Products.ProductName, 
      Products.Price
```

Example from a College-style schema:

```text
Classes
| where Classes.Class_Code == "MATH101"
| navigate set Classes.Enrollment
      return Students.StudentID,
             Students.Advisor
| navigate Teachers on Students.Advisor == Teachers.Employee_ID
      return Teachers.Name
```

In the second example:

1. The pipeline starts from `Classes` and filters to `MATH101`.
2. It navigates the `Enrollment` SET and produces an in-memory result containing student identifiers and advisor references.
3. It then follows the student advisor FK to the `Teachers` table and returns teacher names.

The final result is the list of advisors for students attending that class. Depending on execution semantics, duplicates may remain unless duplicate elimination is defined as part of the engine.

## Document Map

The rest of the design has been split into focused documents:

- [Formal Language Description](./FormalLanguage.md)
- [Query Engine Notes](./QueryEngine.md)