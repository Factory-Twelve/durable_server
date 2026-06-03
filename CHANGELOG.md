## 0.1.3 (2026-06-03)
- Return `{:error, {:unreachable, pid}}` from `ensure_started_child/2` when storage shows a live owner but Group metadata is unavailable.
- Avoid an extra storage read on restart paths by reusing preloaded object data.
- Reserve restart ownership during `rehome_child/3` so LifecycleManager cannot claim the child between shutdown and replacement placement.
- Singleflight concurrent `rehome_child/3` calls per key/module to avoid redundant shutdown/start writes.
- Let callers waiting on an active restart claim make one final takeover retry before timing out, so expired orphaned rehome attempts do not require a second external call.
- Handle `:ignore` from user `init/2` in the local `ensure_started_child/3` path instead of raising `CaseClauseError`.

## 0.1.2 (2026-05-14)
- Fix orphan claim logic allowing dueling LifecycleManager claim

## 0.1.1 (2026-04-29) 🚀
- Initial release!
