# AGENTS.md

## Tech stack
- Python (use as much as possible)
    - python (3.13) via uv only (always use uv add or uv pip etc.). 
    - 'uv format' to format code (works like ruff format)
    - 'uvx ty check' to type check code (new 2026 feature)
    - Logging: loguru
    - notebooks: marimo (don't use jupyter)
    - LLM agents: prefer smolagents (documentation at https://huggingface.co/docs/smolagents/en/reference/agents), litellm (only if needed)
    - Data analysis: numpy (for simple analysis), polars (for large complex analysis)
    - Simple frontend: gradio
    - web api (only if needed): fastapi
    - async jobs: taskiq (for background / long-running tasks)
    - complex content files: unstructured
    - Database: sqlmodel (documented at https://sqlmodel.tiangolo.com/) 
- TypeScript / Node.js (only when explicitly requested)
    - Language: TypeScript is mandatory for all Node.js code.
    - Version management: Use mise to manage Node.js versions.
    - Architecture: Default to JAMSTACK static web apps. Ask explicitly before introducing a backend or server-side runtime.
    - Frontend framework: SvelteKit with Tailwind CSS.

## Development approach
- Write **short** correct code using few files, prefer using best-in-class libraries vs. writing custom code
- Focus on ensuring the core flow works well, do not implement fallbacks, mocks, or extensive error handling without specific request 
- If you need to check how a function or module works, put one line notes into "library-learnings.md". I'm often using the latest library versions

- If I ask for a PROOF, provide a PROOF of the currently being worked on feature:
    - A PROOF is a short seperate program that lives in /proofs who's name clearly indicates what functionality it's proving
    - A PROOF provides granular text based output showing what critical steps are being taken, and shows enough output to prove that each step is doing the required output (it also logs the output to a file)
    - PROOF should idempotent without significant side effects (ok to make LLM calls etc.). A PROOF NEVER mocks output or libraries without EXPLICIT permission and guidence

