import os
from pathlib import Path
from typing import Iterable, List, Tuple

import pandas as pd


DATA_DIR = Path(__file__).resolve().parents[3] / "data"
CLEANED_ROOT = Path(__file__).resolve().parents[3] / "cleaned-dataset"
LOG_FILE = Path(__file__).resolve().parent / "removed_100000_entries.txt"


def _state_district_columns(columns: Iterable[str]) -> Tuple[str, str]:
	"""Return probable state and district column names (case-insensitive)."""
	lower_cols = {c.lower(): c for c in columns}
	state_col = next((lower_cols[c] for c in lower_cols if "state" in c), None)
	district_col = next((lower_cols[c] for c in lower_cols if "district" in c), None)
	return state_col, district_col


def _write_log(lines: List[str]) -> None:
	LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
	with LOG_FILE.open("a", encoding="utf-8") as f:
		for line in lines:
			f.write(line)


def _process_file(csv_path: Path) -> None:
	rel_path = csv_path.relative_to(DATA_DIR)
	target_path = CLEANED_ROOT / rel_path
	target_path.parent.mkdir(parents=True, exist_ok=True)

	first_chunk = True
	removed_any = False
	log_lines: List[str] = []

	for chunk in pd.read_csv(csv_path, chunksize=100_000, dtype=str):
		state_col, district_col = _state_district_columns(chunk.columns)
		if not state_col and not district_col:
			if first_chunk:
				print(f"[SKIP] No state/district columns in {rel_path}")
			chunk.to_csv(target_path, mode="a", index=False, header=first_chunk)
			first_chunk = False
			continue

		mask = False
		if state_col:
			mask = mask | (chunk[state_col] == "100000")
		if district_col:
			mask = mask | (chunk[district_col] == "100000")

		removed_rows = chunk[mask]
		kept_rows = chunk[~mask]

		if not removed_rows.empty:
			removed_any = True
			print(f"[REMOVE] {len(removed_rows)} rows from {rel_path}")
			log_lines.append(f"FILE: {rel_path}\n")
			log_lines.append(removed_rows.to_csv(sep="\t", index=False))
			log_lines.append("\n")

		kept_rows.to_csv(target_path, mode="a", index=False, header=first_chunk)
		first_chunk = False

	if removed_any and log_lines:
		_write_log(log_lines)
	if not removed_any:
		print(f"[CLEAN] No rows with value 100000 in {rel_path}")


def main() -> None:
	if not DATA_DIR.exists():
		raise FileNotFoundError(f"Data directory not found: {DATA_DIR}")

	if CLEANED_ROOT.exists():
		print(f"[INFO] Cleaned dataset dir exists: {CLEANED_ROOT}")
	else:
		CLEANED_ROOT.mkdir(parents=True)
		print(f"[INFO] Created cleaned dataset dir: {CLEANED_ROOT}")

	if LOG_FILE.exists():
		LOG_FILE.unlink()
		print(f"[INFO] Cleared previous log: {LOG_FILE}")

	csv_files = [p for p in DATA_DIR.rglob("*.csv") if p.is_file()]
	if not csv_files:
		print(f"[WARN] No CSV files found under {DATA_DIR}")
		return

	print(f"[INFO] Found {len(csv_files)} CSV files under {DATA_DIR}")
	for csv_file in csv_files:
		_process_file(csv_file)

	print(f"[DONE] Finished cleaning. Cleaned files in {CLEANED_ROOT}")
	print(f"[LOG] Removed entries logged to {LOG_FILE}")


if __name__ == "__main__":
	main()
