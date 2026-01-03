from __future__ import annotations

from pathlib import Path
from typing import Dict, List

from .output import write_jsonl, write_manifest, write_watchlist_json, write_watchlist_txt
from .util import ensure_dir


def sort_candidates(records: List[Dict]) -> List[Dict]:
    return sorted(
        records,
        key=lambda r: (
            -(r.get("score") or 0),
            -(r.get("adv_jpy") or 0),
            -(r.get("adtv_shares") or 0),
            r.get("ticker") or "",
        ),
    )


def select_watchlist(
    scored: List[Dict],
    select_top: int,
    min_score: int,
    min_adv_jpy: int,
    strict: bool,
) -> Dict[str, List[Dict]]:
    candidates: List[Dict] = []
    missing: List[Dict] = []

    for rec in scored:
        status_ok = rec.get("status") == "OK"
        enough_score = (rec.get("score") or 0) >= min_score
        enough_adv = (rec.get("adv_jpy") or 0) >= min_adv_jpy
        if status_ok and enough_score and enough_adv:
            candidates.append(rec)
        else:
            missing.append(rec)

    sorted_candidates = sort_candidates(candidates)
    watchlist = sorted_candidates[:select_top]
    return {"watchlist": watchlist, "missing": missing, "candidates": sorted_candidates}


def emit_watchlist_outputs(
    watchlist: List[Dict],
    scored: List[Dict],
    missing: List[Dict],
    outdir: Path,
    watchlist_dir: Path,
    manifest_name: str,
) -> None:
    ensure_dir(outdir)
    ensure_dir(watchlist_dir)

    write_watchlist_txt(watchlist_dir / "watchlist.txt", [r["ticker"] for r in watchlist])
    write_watchlist_json(watchlist_dir / "watchlist.json", watchlist)
    write_jsonl(outdir / "weekly_scores.jsonl", scored)

    manifest = {
        "universe_count": len(scored),
        "watchlist_count": len(watchlist),
        "missing_count": len(missing),
        "output": {
            "watchlist_txt": str((watchlist_dir / "watchlist.txt").as_posix()),
            "watchlist_json": str((watchlist_dir / "watchlist.json").as_posix()),
            "scores": str((outdir / "weekly_scores.jsonl").as_posix()),
        },
    }
    write_manifest(outdir / manifest_name, manifest)
