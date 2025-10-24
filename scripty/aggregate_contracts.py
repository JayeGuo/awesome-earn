#!/usr/bin/env python3
"""Aggregate contract cost and profit metrics from trading CSV data."""

from __future__ import annotations

import argparse
import csv
from collections import OrderedDict
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path
from typing import Dict, Iterable, Tuple


HEADER = ["合约", "总成本", "总利润", "收益率"]
DEFAULT_INPUT_ORDER = [
    Path("/in/test.csv"),
    Path("in/test.csv"),
    Path("scripty/in/test.csv"),
]
DEFAULT_OUTPUT_BASES = [
    Path("/out"),
    Path("out"),
    Path("scripty/out"),
]


class ContractAggregation:
    """Track cumulative cost and profit for a single contract."""

    __slots__ = ("cost", "profit")

    def __init__(self) -> None:
        self.cost: Decimal = Decimal("0")
        self.profit: Decimal = Decimal("0")

    def add_cost(self, amount: Decimal) -> None:
        self.cost += amount

    def add_profit(self, amount: Decimal) -> None:
        self.profit += amount

    def yield_ratio(self) -> Decimal:
        if self.cost == 0:
            return Decimal("0")
        return self.profit / self.cost


def parse_decimal(value: str | None) -> Decimal:
    """Convert a string to Decimal while tolerating blanks and commas."""
    if value is None:
        return Decimal("0")
    text = value.strip()
    if not text:
        return Decimal("0")
    text = text.replace(",", "")
    try:
        return Decimal(text)
    except InvalidOperation:
        return Decimal("0")


def find_existing_path(candidates: Iterable[Path]) -> Path:
    for path in candidates:
        if path.exists():
            return path
    # No candidate exists; return the first one so caller knows default target
    return next(iter(candidates))


def ensure_output_path(custom: str | None) -> Path:
    if custom:
        output_path = Path(custom)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path

    for base_dir in DEFAULT_OUTPUT_BASES:
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
        except OSError:
            continue
        if base_dir.exists():
            return base_dir / "contract_summary.csv"

    raise FileNotFoundError("Unable to create any output directory")


def resolve_input_path(custom: str | None) -> Path:
    if custom:
        path = Path(custom)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found: {path}")
        return path

    candidate = find_existing_path(DEFAULT_INPUT_ORDER)
    if not candidate.exists():
        raise FileNotFoundError(
            "Input CSV not found. Checked: "
            + ", ".join(str(p) for p in DEFAULT_INPUT_ORDER)
        )
    return candidate


def aggregate_contracts(rows: Iterable[Dict[str, str]]) -> OrderedDict[str, ContractAggregation]:
    totals: "OrderedDict[str, ContractAggregation]" = OrderedDict()

    for row in rows:
        contract = row.get("合约", "").strip()
        if not contract:
            continue

        pnl = parse_decimal(row.get("已实现盈亏"))
        amount = parse_decimal(row.get("成交额"))

        bucket = totals.setdefault(contract, ContractAggregation())

        if pnl == 0:
            bucket.add_cost(amount)
        else:
            bucket.add_profit(pnl)

    return totals


def load_rows(path: Path) -> Iterable[Dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        missing = {field for field in ("合约", "成交额", "已实现盈亏") if field not in reader.fieldnames or reader.fieldnames is None}
        if missing:
            raise ValueError(f"Input CSV 缺少必需字段: {', '.join(sorted(missing))}")
        for row in reader:
            yield row


def format_decimal(value: Decimal, digits: int = 8) -> str:
    quantize_pattern = Decimal("1." + ("0" * digits))
    return str(value.quantize(quantize_pattern, rounding=ROUND_HALF_UP))


def write_output(path: Path, results: OrderedDict[str, ContractAggregation]) -> None:
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(HEADER)
        for contract, aggregation in results.items():
            yield_ratio = aggregation.yield_ratio()
            writer.writerow([
                contract,
                format_decimal(aggregation.cost, digits=6),
                format_decimal(aggregation.profit, digits=6),
                format_decimal(yield_ratio, digits=6),
            ])


def main(argv: Iterable[str] | None = None) -> Tuple[Path, Path]:
    parser = argparse.ArgumentParser(description="按合约聚合成本与利润数据")
    parser.add_argument("input", nargs="?", help="输入 CSV 文件路径，默认尝试 /in/test.csv 等")
    parser.add_argument("output", nargs="?", help="输出 CSV 文件路径，默认写入 /out/contract_summary.csv")

    args = parser.parse_args(argv)

    input_path = resolve_input_path(args.input)
    output_path = ensure_output_path(args.output)

    rows = list(load_rows(input_path))
    results = aggregate_contracts(rows)
    write_output(output_path, results)

    return input_path, output_path


if __name__ == "__main__":
    try:
        input_path, output_path = main()
        print(f"汇总完成: 输入 {input_path} -> 输出 {output_path}")
    except Exception as exc:  # pragma: no cover - command line entry guard
        print(f"处理失败: {exc}")
        raise
