//! Convenção de chaves Redis — espelho de `app/keys.py` no motor Python.
//! Documentada em contracts/redis-keys.md.

pub const EVENTS_STREAM: &str = "rec:events";
pub const JOBS_STREAM: &str = "rec:jobs:rebuild";
pub const TRENDING: &str = "rec:trending";

pub fn catalog(cid: i64) -> String {
    format!("rec:catalog:{cid}")
}
pub fn stats(cid: i64) -> String {
    format!("rec:stats:{cid}")
}
pub fn feed(uid: i64) -> String {
    format!("rec:feed:{uid}")
}
pub fn feed_info(uid: i64) -> String {
    format!("rec:feed:info:{uid}")
}
pub fn feed_meta(uid: i64) -> String {
    format!("rec:feed:meta:{uid}")
}
pub fn profile_meta(uid: i64) -> String {
    format!("rec:profile:meta:{uid}")
}
pub fn profile_cat(uid: i64) -> String {
    format!("rec:profile:cat:{uid}")
}
pub fn profile_tag(uid: i64) -> String {
    format!("rec:profile:tag:{uid}")
}
pub fn profile_author(uid: i64) -> String {
    format!("rec:profile:author:{uid}")
}
pub fn seen(uid: i64) -> String {
    format!("rec:seen:{uid}")
}
pub fn lock_build(uid: i64) -> String {
    format!("rec:lock:build:{uid}")
}
pub fn job_pending(uid: i64) -> String {
    format!("rec:job:pending:{uid}")
}
pub fn rate_limit(scope: &str, who: &str, window: u64) -> String {
    format!("rec:rl:{scope}:{who}:{window}")
}
