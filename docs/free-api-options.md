# Free API Options (Recommended)

Use these to populate:
- `SPORTS_API_URL`
- `SPORTS_API_KEY`
- `NEWS_API_URL`
- `NEWS_API_KEY`
- `LLM_ENDPOINT`
- `LLM_API_KEY`

## Sports Data

### TheSportsDB (community test key available)
- Site: https://www.thesportsdb.com/api.php
- Notes:
  - Has a public test key for basic development.
  - For production reliability/rate limits, create an account and use a private key.

Suggested mapping:
- `SPORTS_API_URL=https://www.thesportsdb.com/api/v1/json/<KEY>`
- `SPORTS_API_KEY=<KEY>`

## News Data

### NewsAPI
- Site: https://newsapi.org/
- Notes:
  - Free developer plan available.
  - Good for headlines; verify terms/rate limits for your production use.

Suggested mapping:
- `NEWS_API_URL=https://newsapi.org/v2`
- `NEWS_API_KEY=<YOUR_KEY>`

### GNews
- Site: https://gnews.io/
- Notes:
  - Free tier available with daily limits.
  - Useful backup source if you want dual-provider redundancy.

Suggested mapping:
- `NEWS_API_URL=https://gnews.io/api/v4`
- `NEWS_API_KEY=<YOUR_KEY>`

## LLM Endpoint

### Groq
- Docs: https://console.groq.com/docs/overview
- Notes:
  - Fast inference and free developer usage tiers.

Suggested mapping:
- `LLM_ENDPOINT=https://api.groq.com/openai/v1/chat/completions`
- `LLM_API_KEY=<YOUR_KEY>`

### OpenRouter (free models available)
- Docs: https://openrouter.ai/docs
- Notes:
  - Access to multiple providers; includes some free models.

Suggested mapping:
- `LLM_ENDPOINT=https://openrouter.ai/api/v1/chat/completions`
- `LLM_API_KEY=<YOUR_KEY>`

## Important

- Do not commit real keys to git.
- Generate local app/database secrets with:
  - `./scripts/generate_local_env.sh`
- Apply generated values as Kubernetes secret with:
  - `./scripts/generate_k8s_secret_from_env.sh .env sideline-wire-dailycast slw-dailycast-secrets`
