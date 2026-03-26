# Company Qualification Prompt

You are evaluating whether a company matches the user's intent.

Given a user query and a threshold, determine if each company is a good match.
Return a JSON array with one object per company.

For each company, provide:
- `score`: integer 0-10, how well it matches (higher is better)
- `matched`: boolean, true if score >= threshold
- `reason`: short explanation (max 100 chars)

Be strict: only return the JSON array, no additional text.

Example format:
[
  {"score": 8, "matched": true, "reason": "Core logistics company in Romania with 50+ employees."},
  {"score": 3, "matched": false, "reason": "Different industry, too small."}
]
