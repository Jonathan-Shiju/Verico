# Investigation

One review of one stored lead, off the payment stream. `Investigator.py` sends that lead to the model named in `.env` and can write keep or dismiss. `Decisions.py` stores the decision in `aml:decisions`, with the reason, the accounts, and the payment ids. The operator session records the decision the investigator states. The case book is written from Operator after a keep.
