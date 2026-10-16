# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Users

The site serves two people in order. First, someone evaluating Verico in a review, a demo, or a presentation. Then, an investigator who opens a lead and works it in the workspace. The explanation comes before the workspace, and the workspace is the proof.

## Product Purpose

Verico is an investigation assistant for connected payments. It turns a stream of payments into leads a person can read, walks the payments connected to a lead, and writes a case book only when the investigator keeps the lead and asks for the record. Success on this site is completing that work: read a lead, walk the payments, keep or dismiss, and receive a case book only after a keep.

## Positioning

Verico prepares the connected record and the lead. It does not decide that a payment is fraud. A mark on an account with many counterparties is not itself a lead. A dismiss does not open a case book.

## Operating Context

The site has three routes: Idea (`/`), Method (`/method`), and Workspace (`/investigate`). The workspace talks to the local demo API. The presentation data is a cut of the HI-Small payment file, not the full stream. Evaluators may arrive with no prior case file. Investigators use the same workspace to open a lead, ask about the accounts that dealt with one already open, and read who paid whom.

## Capabilities and Constraints

Confirmed behavior the revamp keeps:

- Payments are kept as accounts joined by those payments.
- The stream is reduced to the payments that connect.
- Leads are money that left an account and later came back, and payments that add up to one another.
- An account with many counterparties can be marked. That mark is not a lead.
- The investigator opens a lead or asks for the accounts that dealt with one already open.
- The workspace follows the connected payments.
- A case book is written only when the investigator keeps the lead and asks for the record.
- Verico does not call a payment fraud.

Copy and page structure may be rewritten. Further changes to product behavior are undecided.

## Brand Commitments

The product name is Verico. No other identity, voice, or asset was made binding.

## Evidence on Hand

- Site copy and behavior in `web/app`: Idea, Method, and Workspace.
- Presentation slice, saved tool responses, operator-pipeline outputs, and terminal logs under `presentation/`.
- Measured pipeline notes are not on the site. Do not invent customers, benchmarks, prices, or a fraud verdict.

## Product Principles

- The person decides. Verico prepares and records; it does not convict a payment.
- A lead is a specific pattern, not a busy account.
- The written record exists only after a keep and a request.
- The explanation has to be true enough that the workspace can prove it.
- Show the path of payments, not a pile of rows.
