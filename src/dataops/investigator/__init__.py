"""Read-only AI incident investigator (Milestone 4).

Modules
  embeddings  embedding providers (configurable; the default is labelled non-neural)
  runbooks    runbook ingestion, section-aware chunking, incremental pgvector index, search
  tools       allowlisted, parameterized, read-only tools the model may call
  evidence    evidence-id resolution against stored records
  schema      structured output schema
  validate    citation, numeric-grounding, action-policy and abstention checks
  providers   LLM providers: Anthropic Messages API, and a labelled deterministic fixture
  engine      the bounded investigation loop and its audit record
"""
