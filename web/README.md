# web

The site an investigator opens. `/` is the front page. `/method` walks through how a stream of payments becomes a case book. `/investigate` is the workspace. The workspace talks to the chat API from `scripts/demo_api.py`, on port `8787` unless `DEMO_API_URL` is set. `next.config.mjs` reads `DEMO_API_URL`, `DEMO_API_UPSTREAM`, and `NEXT_PUBLIC_DEMO_API` from the repo `.env`.

```bash
npm install
npm run dev
```
