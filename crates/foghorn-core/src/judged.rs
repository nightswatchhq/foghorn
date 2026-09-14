//! The one definition of a judged probe answer: whose it is, and whether it was wrong.
//!
//! A gateway observation's `indexer_address` is the allocation signing key and resolves through
//! `allocation_map`; a paid one is already the indexer. An answer nobody can attribute, and a
//! refused payment, are evidence about no indexer. A fault is an answer that differs from the
//! largest cluster by count when that cluster holds more than half of the probe's answers.

/// `WITH responders AS (..), judged AS (..)` over probes dispatched after `since`, an SQL
/// expression. With `excuse_nondeterministic`, answers on flagged deployments are never faults.
/// The detector that maintains the flag passes `false`: excused, a flagged deployment would stop
/// producing the evidence that keeps it flagged, and the flag would flap.
pub fn judged_probes(since: &str, excuse_nondeterministic: bool) -> String {
    let excused = if excuse_nondeterministic {
        "AND nd.deployment_id IS NULL"
    } else {
        ""
    };
    format!(
        r#"
    WITH responders AS (
        SELECT o.probe_id, COUNT(*) FILTER (WHERE o.response_hash IS NOT NULL) AS n
        FROM observation o
        JOIN probe p ON p.id = o.probe_id
        WHERE p.dispatched_at > {since}
        GROUP BY o.probe_id
    ),
    judged AS (
        SELECT COALESCE(am.indexer_address, o.indexer_address) AS indexer_address,
               p.id AS probe_id, p.deployment_id, p.query_category, p.dispatched_at,
               o.latency_ms, o.response_hash,
               COALESCE(
                   o.response_hash <> d.largest_by_count_hash
                   AND d.largest_by_count_size * 2 > r.n
                   {excused},
                   false
               ) AS fault
        FROM observation o
        JOIN probe p ON p.id = o.probe_id
        LEFT JOIN allocation_map am
          ON am.allocation_key = o.indexer_address
         AND am.indexer_address IS NOT NULL
        LEFT JOIN divergence d ON d.probe_id = o.probe_id
        LEFT JOIN responders r ON r.probe_id = o.probe_id
        LEFT JOIN nondeterministic_deployment nd ON nd.deployment_id = p.deployment_id
        WHERE (am.indexer_address IS NOT NULL OR o.dispatch_mode = 'paid')
          AND (o.error_class IS NULL OR o.error_class NOT LIKE 'payment\_%')
          AND p.dispatched_at > {since}
    )"#
    )
}
