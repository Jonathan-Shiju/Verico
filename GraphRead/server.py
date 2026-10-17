"""MCP server: read the AML window graphs and record a decision for one lead.

Window graphs, loops, cases, and flags are read-only. A keep or dismiss is
written only to the decision graph. A counterparty lead the operator orders
is written only to aml:reviews.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from mcp.server.mcpserver import MCPServer  # noqa: E402

from adapters.graphdb import FalkorGraphDB  # noqa: E402
from GraphRead.explain import explain_transactions as build_explanation  # noqa: E402
from GraphRead.operator_reads import list_leads as read_leads  # noqa: E402
from GraphRead.reviews import open_counterparty_leads as open_reviews  # noqa: E402
from GraphRead.operator_reads import summarize as read_summary  # noqa: E402
from GraphRead.operator_reads import walk_transaction as read_walk  # noqa: E402
from GraphRead.reader import GraphReader  # noqa: E402
from Investigation.Decisions import DecisionStore  # noqa: E402
from Investigation.Investigator import record, review  # noqa: E402

server = MCPServer(
    "aml-graph",
    instructions=(
        "Read AML window graphs and the loops found by the tree detector. "
        "The officer's reply uses ordinary words. You may say loop for money "
        "that left an account and later came back. Do not say residue, pooled, "
        "split, walk, step, depth, window, graph, or tool. Several payments "
        "that add up, or one payment divided into several, are described in "
        "those words.\n\n"
        "Window graphs, loops, cases, and flags cannot be modified.\n\n"
        "Account id. The bank number with leading zeros removed, a colon, and "
        "the account number. No account list is provided here. Look an account "
        "up only when a call names it.\n\n"
        "Window graphs. Each window is its own graph, named aml:txn:{start}. "
        "A window covers 8 days, and a new window starts every 4 days, so "
        "neighboring windows overlap by 4 days and a payment near a boundary "
        "is stored in both. Every registered account is created in a window "
        "before its payments. A payment is an edge "
        "(:Account)-[:TRANSFER {txn_id, ts, amount_paid, payment_currency, "
        "amount_received, receiving_currency, payment_format}]->(:Account). "
        "There is no edge from one window graph into another. The same "
        "account id is what joins them: take the other account on an edge in "
        "one window, then read that id in the previous and next windows. "
        "list_windows returns each window and the watermark. The watermark is "
        "the event time up to which every partition has been written. A "
        "window is closed once the watermark has passed its end. Transfers "
        "after the watermark may still be missing.\n\n"
        "Trees. The detector grows a branch forward in time. Money arriving "
        "at an account can be followed along a later outgoing payment when "
        "that payment is a similar amount, not when it is the sum of several "
        "incoming chains and not when one payment has been split into many "
        "small ones. If a later payment returns to an account already on the "
        "branch, that path is a loop: the accounts, the transaction ids, the "
        "amounts, and the start and end. Loops are stored in the aml:loops "
        "graph as (:Account)-[:IN_LOOP {position}]->(:Loop). A loop is a "
        "short record of a chain the tree followed. It does not remove the "
        "underlying payments from the window graphs. "
        "Loop account ids match the window account ids. A chain of at least two "
        "similar payments that never returns is stored in the same graph with "
        "open set. One payment on its own is not an open chain. case_file "
        "assembles either lead: the payments, the accounts, the entity links, "
        "and the windows.\n\n"
        "Residue cases. After one-to-one matching, two kinds of leftover are "
        "stored in the aml:cases graph. pooled means several incoming payments "
        "from different senders, none of which matches an outgoing payment on "
        "its own, together match that outgoing payment. split means one "
        "incoming payment matches a set of later outgoing payments in the same "
        "way. cases_for_account reads them. Start from a loop or a case. The "
        "transaction ids on it are the payments to read. An account with many "
        "counterparties is not given a case.\n\n"
        "Reads. account_activity returns a short list of an account's payments. "
        "account_transfers pages through the same account until its payments "
        "are exhausted. Falkor caps a single reply, so the full read is several "
        "pages, each starting at that account. account_activity omits payments "
        "an account sends to itself. Pass include_self to see them. Each "
        "account carries an entity id. "
        "other_accounts lists a few other accounts of that same party. The "
        "count can be larger than the list. Accounts stay separate nodes.\n\n"
        "Degree flags. flags_for_account reads fan-in, fan-out, and gather "
        "marks from aml:flags. Those marks mean an account has many "
        "counterparties, or that some outgoing amount sits near a sum of "
        "recent incoming amounts. They are a wide screen. They are not the "
        "case list.\n\n"
        "Decisions. investigate_lead reviews one lead. It reads the case file, "
        "and when the lead is open it follows the last account one hop into "
        "the neighboring windows. It returns keep or dismiss, the accounts to "
        "raise, and a reason that cites transaction ids it read. That record "
        "is written to aml:decisions. decision_for_lead reads it back. A "
        "transaction id is not a search key: start from the lead, or from an "
        "account the lead names. Window graphs are not modified.\n\n"
        "Reasoning graph. explain_transactions takes transaction ids already "
        "in hand. It loads only those payments into a temporary Falkor graph, "
        "then deletes that graph. Payments that share an account come back as "
        "one graph. Payments that never meet come back as separate graphs.\n\n"
        "Operator start. list_leads pages the stored loops and residues. A lead "
        "is that stored record. case_file opens one lead into a case file; "
        "there is not a second catalog of case files. summarize reads every "
        "payment on one account, or the accounts linked by one entity. When a "
        "party has many accounts, the summary lists them and one account is "
        "read next. walk_transaction starts at one payment and follows later "
        "outgoing payments from the account that received it. After one of "
        "these starts, the other tools answer the operator's question.\n\n"
        "Operator decisions. record_decision stores keep or dismiss for the "
        "lead the operator opened. The reason has to cite transaction ids that "
        "exist, and a keep can name only accounts on those payments. This "
        "write goes to aml:decisions.\n\n"
        "Counterparty leads. open_counterparty_leads is used only when the "
        "operator asks to flag an account or to open the accounts that dealt "
        "with it. Each counterparty gets its own lead, stored apart from loops "
        "and residues. case_file opens one of those leads into the payments "
        "between the two accounts. list_leads with kind review pages them. "
        "kind all does not."
    ),
)
reader = GraphReader(FalkorGraphDB())
decisions = DecisionStore(reader.graph_db)


@server.tool()
def list_leads(kind: str = "all", skip: int = 0, limit: int = 3) -> dict:
    """Page the leads an operator can open.

    kind is loop, residue, review, or all. limit is at most 3, and for all it
    applies to loops and to residues separately. review is the counterparty
    leads an operator ordered opened, and it is not included in all. A returned
    lead is opened with case_file. Do not ask for more than 3.
    """
    return read_leads(reader, kind, skip, limit)


@server.tool()
def summarize(account_id: str = "", entity_id: str = "") -> dict:
    """Every payment on one account, or the accounts that share an entity.

    Pass an account id or an entity id. An account summary includes the entity
    link. A large entity lists its accounts and does not dump every payment.
    """
    return read_summary(reader, account_id, entity_id)


@server.tool()
def walk_transaction(txn_id: int, depth: int = 3) -> dict:
    """Later payments that left the account which received this one.

    depth is how many later payments to follow, at most 4. Do not mention
    that limit, or this call, in the reply.
    """
    return read_walk(reader, txn_id, depth)


@server.tool()
def open_counterparty_leads(account_id: str, limit: int = 12) -> dict:
    """Open a lead for each account that paid, or was paid by, this account.

    Call this only when the operator asks to flag an account or to open the
    accounts that dealt with it. Each lead is separate from a loop and from
    payments that add up. Open one afterwards with case_file.
    """
    return open_reviews(reader, account_id, limit)


@server.tool()
def list_windows() -> dict:
    """List the rolling transaction windows and the event-time watermark."""
    return reader.list_windows()


@server.tool()
def query_cypher(graph: str, cypher: str, limit: int = 100) -> dict:
    """Run a read-only Cypher query against one window graph, aml:loops, aml:cases, or aml:flags.

    graph is a name returned by list_windows, aml:loops, aml:cases, aml:flags, aml:decisions, or aml:reviews.
    Bind an account id before matching a transfer. A scan by transaction id times out.
    """
    return reader.query(graph, cypher, limit)


@server.tool()
def account_activity(
    account_id: str, window: str = "", limit: int = 50, include_self: bool = False
) -> dict:
    """Transfers into and out of one account.

    Pass a window graph name to search that graph only. Leave window empty to
    search every window, which follows the account from one graph to the next.
    Self-transfers are omitted unless include_self is true. The result names
    the entity and a short list of that entity's other accounts.
    """
    return reader.account_activity(account_id, window or None, limit, include_self)


@server.tool()
def account_transfers(
    account_id: str, window: str = "", include_self: bool = False
) -> dict:
    """Every payment on one account, in every window unless a window is named.

    The read is paged. A payment stored in two overlapping windows is returned
    once. complete is false when a page timed out. Self-transfers are omitted
    unless include_self is true.
    """
    return reader.account_transfers(account_id, window or None, include_self)


@server.tool()
def one_hop_across_windows(
    account_id: str,
    window: str,
    counterparty_limit: int = 10,
    limit: int = 30,
) -> dict:
    """Counterparties of an account in one window, then those accounts in the
    previous and next windows."""
    return reader.one_hop_across_windows(account_id, window, counterparty_limit, limit)


@server.tool()
def loops_for_account(account_id: str, limit: int = 20) -> dict:
    """Loops the tree detector stored for an account, from the aml:loops graph.

    Account ids match the window graphs.
    """
    return reader.loops_for_account(account_id, limit)


@server.tool()
def cases_for_account(account_id: str, limit: int = 20) -> dict:
    """Pooled and split residues for an account: payments that match only as a set.

    Read these, and the loops, before the degree flags.
    """
    return reader.cases_for_account(account_id, limit)


@server.tool()
def case_file(lead: str) -> dict:
    """Assemble one investigation file for a loop, an open chain, a residue, or a counterparty lead.

    lead is the loop or open-chain key (comma-separated transaction ids), a
    residue id, or a review lead id returned by open_counterparty_leads.
    """
    return reader.case_file(lead)


@server.tool()
def investigate_lead(lead: str) -> dict:
    """Review one loop, open chain, or residue and store keep or dismiss.

    lead is the loop or open-chain key, or a residue id from cases_for_account.
    One call reads that lead only. The decision is written to aml:decisions.
    """
    return review(reader, decisions, lead)


@server.tool()
def record_decision(
    lead: str,
    decision: str,
    reason: str,
    txn_ids: list[int],
    accounts: list[str] | None = None,
) -> dict:
    """Store keep or dismiss for the lead an operator opened.

    decision is keep or dismiss. txn_ids are payments already read.
    accounts is empty when dismissing. The reason must cite each transaction id.
    """
    return record(reader, decisions, lead, decision, accounts or [], txn_ids, reason)


@server.tool()
def decision_for_lead(lead: str) -> dict:
    """The keep or dismiss already stored for a lead, if one exists."""
    return reader.decision_for_lead(lead)


@server.tool()
def explain_transactions(txn_ids: list[int]) -> dict:
    """Build a temporary graph of these payments and return each connected piece.

    txn_ids are payments already read, such as the ids cited in a decision.
    The temporary graph is deleted before this returns. Use the returned
    graphs to explain how those payments connect.
    """
    return build_explanation(reader, txn_ids)


@server.tool()
def flags_for_account(account_id: str, limit: int = 20) -> dict:
    """Tier-1 window flags for an account: fan-in, fan-out, or a payout that
    matches the sum of recent incoming payments."""
    return reader.flags_for_account(account_id, limit)


if __name__ == "__main__":
    server.run()
