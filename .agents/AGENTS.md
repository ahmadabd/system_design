# graphify integration for Antigravity

This project has a pre-built knowledge graph located in `graphify-out/`.

## Querying Codebase Architecture

Before answering questions about the architecture, cross-service relationships, event publishing/subscription topologies, or dependencies:
1. Check if `graphify-out/graph.json` exists.
2. Use `/graphify query "<question>"` to run graph traversals and discover relationships, rather than reading files one-by-one.
3. For finding paths between services or modules, use `/graphify path "<source>" "<target>"`.
4. For explaining specific concepts, use `/graphify explain "<concept>"`.

## Keeping the Graph Updated

When modifying code structures, new services, or introducing new database configurations:
- Re-run the AST extraction and graph compilation incrementally using `/graphify --update`.
- Ensure new nodes and relationships are properly categorized and clustered.
