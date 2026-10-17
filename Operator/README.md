# Operator

The session an investigator talks to. It checks the MCP server in GraphRead, then answers one question about leads and payments. When the investigator says keep or dismiss, the session records that decision and the reason. The case book is the workbook written after a keep, when they ask for it. A dismiss does not open one.

`session.py` loads `.env` and needs `HF_TOKEN`, `GEMINI_API_KEY`, or `OPENAI_API_KEY` for a live answer. `reply.md` is the markdown shape of that reply. `ConversationLog.py` stores the turns outside the transaction windows. `workbook.py` writes the case book under `presentation/cases`. `simulated.py` is a stand-in chat model for one conversation. It chooses tools from the question.
