# Intent Qualification

Design a system that determines whether a company truly satisfies a user query.

Search systems typically retrieve many candidates that appear relevant, but these results are noisy. 
The main difficulty is not retrieving candidates, but deciding which ones actually match the user’s intent.

> [!NOTE]
> Example query: "Find logistics companies in Germany"

A search system may return:
- a freight forwarding company in Germany -> clearly relevant  
- a software company building logistics tools -> partially related  
- a foreign company operating near Germany -> weak or irrelevant  

The problem is not retrieval, but qualification.


# Problem Definition

The system must:
- receive a user query
- evaluate a set of candidate companies
- return a ranked or filtered list of companies that truly match the query

The key distinction is between:
- surface similarity (words match)
- intent satisfaction (meaning matches)

A correct system must prioritize intent.


# Nature of the Data

Company profiles are incomplete and heterogeneous.

Some companies have detailed descriptions, others only minimal metadata.
Some fields may be missing entirely.

> [!WARNING]
> The system cannot rely on any single field being present.

This forces the system to:
- combine multiple weak signals
- remain robust under missing information
- avoid brittle rules that depend on specific fields


# Query Complexity

Queries are not uniform. They vary along a spectrum.

At one end are structured queries:
- constraints are explicit
- filters can be applied directly
- evaluation is mostly deterministic

At the other end are semantic queries:
- intent is implicit
- relationships must be inferred
- evaluation requires reasoning

Examples of reasoning:
- understanding supplier vs consumer roles  
- identifying indirect participation in an industry  
- interpreting vague terms such as "fast-growing" or "competing with"  

A single system must handle both extremes.


# Core Difficulty

The task is fundamentally about decision making under ambiguity.

A company may:
- partially match a query
- match only through indirect signals
- appear similar but be irrelevant

The system must decide:
- which signals matter
- how strongly they matter
- when to trust or ignore incomplete data

This requires balancing:
- precision (avoid false positives)
- recall (avoid missing valid matches)
- efficiency (avoid unnecessary computation)


# Why Naive Approaches Fail

LLM per company:
- evaluates each company independently
- provides strong reasoning

> [!WARNING]
> Does not scale, expensive, slow, and inconsistent on edge cases

Embedding similarity:
- compares vector representations
- fast and scalable

> [!WARNING]
> Cannot distinguish role or intent, often confuses related concepts

Example failure:
- Query: "companies supplying packaging for cosmetics"
- System returns cosmetics brands instead of packaging suppliers

The issue is that similarity captures proximity, not function.


# Objective

Design a system that:
- filters out clearly irrelevant candidates early
- applies deeper reasoning only where necessary
- combines structured signals with semantic understanding
- scales to large numbers of companies

The output must be:
- a ranked or filtered set of companies
- aligned with the true intent of the query

The quality of the system is determined by how well it separates, truly relevant companies from superficially similar but incorrect ones.