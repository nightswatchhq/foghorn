//! Per-indexer alert subscriptions (lodestar#256): what changed since the last look, as messages.
//!
//! Pure. The probe loop fetches a [`Snapshot`] from Lodestar's public API, hands it here with the
//! state it stored last time, delivers the events, and stores the new state only once delivery
//! succeeded, so a failed post is retried rather than lost.

use std::collections::{BTreeMap, HashSet};
use std::net::IpAddr;

/// SubgraphService maxPOIStaleness in days; one epoch is one day on Arbitrum. Lodestar's #249
/// thresholds, so the page and the alert agree on what amber and red mean.
pub const MAX_POI_STALENESS_DAYS: i64 = 28;
pub const POI_AMBER_DAYS_LEFT: i64 = 7;
pub const POI_RED_DAYS_LEFT: i64 = 2;

pub const KINDS: &[&str] = &["poi", "denied", "signal", "cuts", "reo"];

#[derive(Debug, Clone, PartialEq)]
pub struct AllocSnap {
    pub id: String,
    pub ipfs_hash: String,
    pub name: Option<String>,
    pub created_at_epoch: i64,
    pub signal_grt: f64,
    pub denied: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub current_epoch: i64,
    pub allocations: Vec<AllocSnap>,
    /// PPM, as the chain holds them.
    pub indexing_reward_cut: Option<i64>,
    pub query_fee_cut: Option<i64>,
    pub reo_status: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlertEvent {
    pub kind: &'static str,
    pub message: String,
}

pub type State = BTreeMap<String, String>;

fn tone(days_left: i64) -> &'static str {
    if days_left <= POI_RED_DAYS_LEFT {
        "close"
    } else if days_left <= POI_AMBER_DAYS_LEFT {
        "due"
    } else {
        "fresh"
    }
}

fn tone_rank(t: &str) -> u8 {
    match t {
        "close" => 2,
        "due" => 1,
        _ => 0,
    }
}

fn label(a: &AllocSnap) -> String {
    match &a.name {
        Some(n) if !n.is_empty() => format!("{n} ({})", a.ipfs_hash),
        _ => a.ipfs_hash.clone(),
    }
}

fn pct(ppm: i64) -> String {
    format!("{:.2}%", ppm as f64 / 10_000.0)
}

/// Events since `prev`, and the state to store. An empty `prev` is a first look: it records
/// baselines and reports nothing, so subscribing does not replay every allocation's history.
pub fn evaluate(
    prev: &State,
    snap: &Snapshot,
    kinds: &[String],
    signal_move_pct: f64,
) -> (Vec<AlertEvent>, State) {
    let on = |k: &str| kinds.iter().any(|x| x == k);
    let first = prev.is_empty();
    let mut events = Vec::new();
    let mut next = State::new();
    next.insert("seen".into(), "1".into());

    let mut deployments = HashSet::new();
    for a in &snap.allocations {
        let days_left = MAX_POI_STALENESS_DAYS - (snap.current_epoch - a.created_at_epoch).max(0);
        let t = tone(days_left);
        let key = format!("poi:{}", a.id);
        let was = prev.get(&key).map(String::as_str).unwrap_or("fresh");
        if on("poi") && !first && tone_rank(t) > tone_rank(was) {
            let message = if t == "close" && days_left <= 0 {
                format!("POI overdue: {} is {}d past maxPOIStaleness and can be force-closed. Allocation {}.", label(a), -days_left, a.id)
            } else if t == "close" {
                format!("POI due now: {} has {days_left}d before it can be force-closed. Allocation {}.", label(a), a.id)
            } else {
                format!(
                    "POI due soon: {} has {days_left}d left. Allocation {}.",
                    label(a),
                    a.id
                )
            };
            events.push(AlertEvent {
                kind: "poi",
                message,
            });
        }
        next.insert(key, t.to_string());

        if !deployments.insert(a.ipfs_hash.clone()) {
            continue;
        }

        let key = format!("denied:{}", a.ipfs_hash);
        let was_denied = prev.get(&key).is_some_and(|v| v == "1");
        if on("denied") && !first && a.denied != was_denied {
            let message = if a.denied {
                format!("Rewards denied: {} is on the RewardsManager denylist. Allocations there earn nothing.", label(a))
            } else {
                format!("Rewards allowed again: {} is off the denylist.", label(a))
            };
            events.push(AlertEvent {
                kind: "denied",
                message,
            });
        }
        next.insert(key, if a.denied { "1" } else { "0" }.into());

        let key = format!("signal:{}", a.ipfs_hash);
        let baseline = prev.get(&key).and_then(|v| v.parse::<f64>().ok());
        let mut keep = a.signal_grt;
        if let Some(b) = baseline {
            let moved = if b > 0.0 {
                100.0 * (a.signal_grt - b) / b
            } else if a.signal_grt > 0.0 {
                f64::INFINITY
            } else {
                0.0
            };
            if moved.abs() >= signal_move_pct {
                if on("signal") && !first {
                    events.push(AlertEvent {
                        kind: "signal",
                        message: format!(
                            "Signal {} on {}: {:.0} to {:.0} GRT ({}{:.0}%).",
                            if moved > 0.0 { "up" } else { "down" },
                            label(a),
                            b,
                            a.signal_grt,
                            if moved > 0.0 { "+" } else { "" },
                            moved.clamp(-100.0, 9_999.0),
                        ),
                    });
                }
            } else {
                // Measured against the level at the last alert, so a slow drift still arrives.
                keep = b;
            }
        }
        next.insert(key, keep.to_string());
    }

    if let (Some(irc), Some(qfc)) = (snap.indexing_reward_cut, snap.query_fee_cut) {
        let now = format!("{irc}/{qfc}");
        if let Some(was) = prev.get("cuts") {
            if on("cuts") && was != &now {
                let (wi, wq) = was.split_once('/').unwrap_or(("", ""));
                let (wi, wq) = (wi.parse().unwrap_or(0), wq.parse().unwrap_or(0));
                events.push(AlertEvent {
                    kind: "cuts",
                    message: format!(
                        "Cuts changed: indexing rewards {} to {}, query fees {} to {}.",
                        pct(wi),
                        pct(irc),
                        pct(wq),
                        pct(qfc)
                    ),
                });
            }
        }
        next.insert("cuts".into(), now);
    } else if let Some(was) = prev.get("cuts") {
        next.insert("cuts".into(), was.clone());
    }

    match &snap.reo_status {
        Some(now) => {
            if let Some(was) = prev.get("reo") {
                if on("reo") && was != now {
                    events.push(AlertEvent {
                        kind: "reo",
                        message: format!("REO status changed: {was} to {now}."),
                    });
                }
            }
            next.insert("reo".into(), now.clone());
        }
        None => {
            if let Some(was) = prev.get("reo") {
                next.insert("reo".into(), was.clone());
            }
        }
    }

    (events, next)
}

/// Why a webhook URL is refused, before anything is sent to it; otherwise its host and port.
pub fn check_webhook_url(raw: &str) -> Result<(String, u16), &'static str> {
    if raw.len() > 512 {
        return Err("the webhook URL is too long");
    }
    let rest = raw
        .strip_prefix("https://")
        .ok_or("the webhook URL must be https")?;
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    if authority.is_empty() || authority.contains('@') {
        return Err("the webhook URL has no usable host");
    }
    let (host, port) = match authority.strip_prefix('[') {
        Some(v6) => v6
            .split_once(']')
            .map(|(h, rest)| (h, rest.strip_prefix(':')))
            .ok_or("the webhook URL has no usable host")?,
        None => match authority.split_once(':') {
            Some((h, p)) => (h, Some(p)),
            None => (authority, None),
        },
    };
    if host.is_empty()
        || port.is_some_and(|p| p.is_empty() || !p.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err("the webhook URL has no usable host");
    }
    let host = host.to_ascii_lowercase();
    if host == "localhost"
        || host.ends_with(".localhost")
        || host.ends_with(".internal")
        || host.ends_with(".local")
    {
        return Err("the webhook URL points at a private host");
    }
    if let Ok(ip) = host.parse::<IpAddr>() {
        if !is_public(ip) {
            return Err("the webhook URL points at a private address");
        }
    }
    let port = match port {
        Some(p) => p
            .parse()
            .map_err(|_| "the webhook URL has no usable host")?,
        None => 443,
    };
    Ok((host, port))
}

/// POST each body to the webhook, in order, stopping at the first refusal.
///
/// The name is resolved here and the connection pinned to the checked address, so a DNS answer
/// that changes between the check and the connect cannot reach a private host. No redirects.
pub async fn deliver(url: &str, bodies: &[serde_json::Value]) -> Result<(), String> {
    let (host, port) = check_webhook_url(url).map_err(str::to_string)?;
    let addrs: Vec<_> = tokio::net::lookup_host((host.as_str(), port))
        .await
        .map_err(|_| format!("{host} does not resolve"))?
        .collect();
    let addr = match addrs.first() {
        Some(a) if addrs.iter().all(|a| is_public(a.ip())) => *a,
        Some(_) => return Err("the webhook host resolves to a private address".into()),
        None => return Err(format!("{host} does not resolve")),
    };
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(10))
        .resolve(&host, addr)
        .build()
        .map_err(|e| e.to_string())?;
    for body in bodies {
        let resp = client
            .post(url)
            .json(body)
            .send()
            .await
            .map_err(|_| "the webhook did not answer".to_string())?;
        if !resp.status().is_success() {
            return Err(format!("the webhook answered {}", resp.status().as_u16()));
        }
    }
    Ok(())
}

/// Whether an address may be posted to. Checked against every resolved address, at subscribe time
/// and again at each delivery, since a name can be pointed elsewhere after it was accepted.
pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_unspecified()
                || v4.is_broadcast()
                || v4.is_documentation()
                || o[0] == 0
                || (o[0] == 100 && (64..128).contains(&o[1]))
                || (o[0] == 198 && (o[1] == 18 || o[1] == 19))
                || o[0] >= 224)
        }
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public(IpAddr::V4(v4));
            }
            let s = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (s[0] & 0xfe00) == 0xfc00
                || (s[0] & 0xffc0) == 0xfe80)
        }
    }
}

/// A bulk reallocation turns hundreds of allocations at once, and Discord rate-limits a webhook to
/// a few posts a second; past this many lines of one kind it says how many more there are.
const DISCORD_LINES_PER_KIND: usize = 5;

pub fn is_discord(url: &str) -> bool {
    url.starts_with("https://discord.com/api/webhooks/")
        || url.starts_with("https://discordapp.com/api/webhooks/")
}

/// The request bodies for one delivery. Discord gets `content` chunked under its 2,000 characters;
/// anything else gets one JSON document with the events as data.
pub fn payloads(url: &str, indexer: &str, events: &[AlertEvent]) -> Vec<serde_json::Value> {
    let link = format!("https://lodestar-dashboard.com/indexers/{indexer}?tab=allocations");
    if !is_discord(url) {
        return vec![serde_json::json!({
            "indexer": indexer,
            "url": link,
            "events": events.iter().map(|e| serde_json::json!({ "kind": e.kind, "message": e.message })).collect::<Vec<_>>(),
        })];
    }
    let header = format!("**Lodestar alerts for {indexer}**");
    let mut lines = Vec::new();
    for kind in KINDS.iter().copied().chain(["test"]) {
        let of_kind: Vec<_> = events.iter().filter(|e| e.kind == kind).collect();
        lines.extend(
            of_kind
                .iter()
                .take(DISCORD_LINES_PER_KIND)
                .map(|e| e.message.clone()),
        );
        if of_kind.len() > DISCORD_LINES_PER_KIND {
            lines.push(format!(
                "and {} more {kind} alerts on the page",
                of_kind.len() - DISCORD_LINES_PER_KIND
            ));
        }
    }
    let mut out = Vec::new();
    let mut body = header.clone();
    for line in lines {
        let line = format!("\n- {line}");
        if body.len() + line.len() > 1_800 {
            out.push(body);
            body = header.clone();
        }
        body.push_str(&line);
    }
    body.push_str(&format!("\n{link}"));
    out.push(body);
    out.into_iter()
        .map(|content| serde_json::json!({ "content": content }))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alloc(id: &str, hash: &str, epoch: i64, signal: f64) -> AllocSnap {
        AllocSnap {
            id: id.into(),
            ipfs_hash: hash.into(),
            name: None,
            created_at_epoch: epoch,
            signal_grt: signal,
            denied: false,
        }
    }

    fn snap(allocations: Vec<AllocSnap>) -> Snapshot {
        Snapshot {
            current_epoch: 100,
            allocations,
            indexing_reward_cut: Some(100_000),
            query_fee_cut: Some(0),
            reo_status: Some("eligible".into()),
        }
    }

    fn all() -> Vec<String> {
        KINDS.iter().map(|k| k.to_string()).collect()
    }

    #[test]
    fn first_look_records_baselines_and_says_nothing() {
        let (events, state) = evaluate(
            &State::new(),
            &snap(vec![alloc("0xa", "QmA", 75, 1000.0)]),
            &all(),
            20.0,
        );
        assert!(events.is_empty());
        assert_eq!(state.get("poi:0xa").map(String::as_str), Some("due"));
        assert_eq!(state.get("cuts").map(String::as_str), Some("100000/0"));
    }

    #[test]
    fn poi_alerts_once_per_step_worse() {
        let kinds = all();
        let (_, s) = evaluate(
            &State::new(),
            &snap(vec![alloc("0xa", "QmA", 80, 1000.0)]),
            &kinds,
            20.0,
        );
        let (e, s) = evaluate(
            &s,
            &snap(vec![alloc("0xa", "QmA", 75, 1000.0)]),
            &kinds,
            20.0,
        );
        assert_eq!(e.len(), 1);
        assert!(e[0].message.starts_with("POI due soon"));
        let (e, s) = evaluate(
            &s,
            &snap(vec![alloc("0xa", "QmA", 75, 1000.0)]),
            &kinds,
            20.0,
        );
        assert!(e.is_empty());
        let (e, s) = evaluate(
            &s,
            &snap(vec![alloc("0xa", "QmA", 73, 1000.0)]),
            &kinds,
            20.0,
        );
        assert!(e[0].message.starts_with("POI due now: QmA has 1d"));
        let (e, _) = evaluate(&State::new(), &snap(vec![]), &kinds, 20.0);
        assert!(e.is_empty());
        let mut overdue = State::new();
        overdue.insert("seen".into(), "1".into());
        let (e, _) = evaluate(
            &overdue,
            &snap(vec![alloc("0xb", "QmB", 69, 1000.0)]),
            &kinds,
            20.0,
        );
        assert!(e[0]
            .message
            .starts_with("POI overdue: QmB is 3d past maxPOIStaleness"));
        // Closed and gone: its key goes with it.
        let (e, s) = evaluate(&s, &snap(vec![]), &kinds, 20.0);
        assert!(e.is_empty());
        assert!(!s.contains_key("poi:0xa"));
    }

    #[test]
    fn denial_alerts_on_both_transitions() {
        let kinds = all();
        let mut a = alloc("0xa", "QmA", 99, 1000.0);
        let (_, s) = evaluate(&State::new(), &snap(vec![a.clone()]), &kinds, 20.0);
        a.denied = true;
        let (e, s) = evaluate(&s, &snap(vec![a.clone()]), &kinds, 20.0);
        assert_eq!(e[0].kind, "denied");
        a.denied = false;
        let (e, _) = evaluate(&s, &snap(vec![a]), &kinds, 20.0);
        assert!(e[0].message.starts_with("Rewards allowed again"));
    }

    #[test]
    fn signal_drift_accumulates_against_the_last_alert() {
        let kinds = all();
        let (_, s) = evaluate(
            &State::new(),
            &snap(vec![alloc("0xa", "QmA", 99, 1000.0)]),
            &kinds,
            20.0,
        );
        let (e, s) = evaluate(
            &s,
            &snap(vec![alloc("0xa", "QmA", 99, 1100.0)]),
            &kinds,
            20.0,
        );
        assert!(e.is_empty());
        let (e, s) = evaluate(
            &s,
            &snap(vec![alloc("0xa", "QmA", 99, 1250.0)]),
            &kinds,
            20.0,
        );
        assert_eq!(e[0].message, "Signal up on QmA: 1000 to 1250 GRT (+25%).");
        let (e, _) = evaluate(
            &s,
            &snap(vec![alloc("0xa", "QmA", 99, 1300.0)]),
            &kinds,
            20.0,
        );
        assert!(e.is_empty());
    }

    #[test]
    fn cuts_and_reo_changes_and_unknowns_keep_the_old_value() {
        let kinds = all();
        let (_, s) = evaluate(&State::new(), &snap(vec![]), &kinds, 20.0);
        let mut changed = snap(vec![]);
        changed.indexing_reward_cut = Some(1_000_000);
        changed.reo_status = Some("ineligible".into());
        let (e, s) = evaluate(&s, &changed, &kinds, 20.0);
        assert_eq!(
            e.iter().map(|e| e.kind).collect::<Vec<_>>(),
            vec!["cuts", "reo"]
        );
        assert_eq!(
            e[0].message,
            "Cuts changed: indexing rewards 10.00% to 100.00%, query fees 0.00% to 0.00%."
        );
        let mut unknown = changed.clone();
        unknown.reo_status = None;
        unknown.indexing_reward_cut = None;
        let (e, s) = evaluate(&s, &unknown, &kinds, 20.0);
        assert!(e.is_empty());
        assert_eq!(s.get("reo").map(String::as_str), Some("ineligible"));
        assert_eq!(s.get("cuts").map(String::as_str), Some("1000000/0"));
    }

    #[test]
    fn only_chosen_kinds_are_reported() {
        let (_, s) = evaluate(&State::new(), &snap(vec![]), &all(), 20.0);
        let mut changed = snap(vec![]);
        changed.reo_status = Some("ineligible".into());
        let (e, _) = evaluate(&s, &changed, &["poi".to_string()], 20.0);
        assert!(e.is_empty());
    }

    #[test]
    fn webhook_urls_must_be_public_https() {
        assert!(check_webhook_url("https://discord.com/api/webhooks/1/abc").is_ok());
        assert!(check_webhook_url("https://hooks.example.com:8443/x").is_ok());
        assert!(check_webhook_url("http://discord.com/api/webhooks/1/abc").is_err());
        assert!(check_webhook_url("https://localhost/x").is_err());
        assert!(check_webhook_url("https://127.0.0.1/x").is_err());
        assert!(check_webhook_url("https://10.1.2.3/x").is_err());
        assert!(check_webhook_url("https://169.254.169.254/latest").is_err());
        assert!(check_webhook_url("https://[::1]/x").is_err());
        assert!(check_webhook_url("https://[::ffff:127.0.0.1]/x").is_err());
        assert!(check_webhook_url("https://user@example.com/x").is_err());
        assert!(check_webhook_url("https://metadata.google.internal/x").is_err());
    }

    #[test]
    fn discord_messages_stay_under_the_limit() {
        let events: Vec<_> = (0..100)
            .map(|i| AlertEvent {
                kind: "poi",
                message: format!("POI due soon: Qm{i:044} has 5d left. Allocation 0x{i:040}."),
            })
            .collect();
        let bodies = payloads("https://discord.com/api/webhooks/1/abc", "0xabc", &events);
        assert_eq!(bodies.len(), 1);
        let content = bodies[0]["content"].as_str().unwrap();
        assert!(content.len() < 2_000);
        assert!(content.contains("and 95 more poi alerts"));
        let generic = payloads("https://hooks.example.com/x", "0xabc", &events);
        assert_eq!(generic.len(), 1);
        assert_eq!(generic[0]["events"].as_array().unwrap().len(), 100);
    }
}
