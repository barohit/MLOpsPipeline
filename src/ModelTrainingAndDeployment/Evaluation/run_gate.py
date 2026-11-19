"""Evaluation gate, run in CI before any rollout.

    python -m src.ModelTrainingAndDeployment.Evaluation.run_gate \
        --holdout-dir holdouts/v1 \
        --candidate-uri models:/team_regular_season_win_count_linear_regression/7 \
        --production-uri models:/team_regular_season_win_count_linear_regression@champion \
        --report gate_report.json

Exit codes: 0 = passed, 1 = failed the gate, 2 = could not evaluate.
"""
import argparse
import json
import sys

from src.ModelTrainingAndDeployment.Evaluation.evaluation import EvaluationGate, evaluate
from src.ModelTrainingAndDeployment.Evaluation.holdout import load_holdout
from src.ModelTrainingAndDeployment.ModelRegistries.model_loader import load_model


def check_candidate_matches_holdout(candidate_metadata: dict, manifest: dict) -> list[str]:
    """The candidate must have been trained with this exact holdout excluded,
    and on the same feature list the holdout was frozen with."""
    problems = []

    if candidate_metadata.get("holdout_sha256") != manifest["sha256"]:
        problems.append(
            "Candidate was not trained against this holdout version "
            f"(model says {candidate_metadata.get('holdout_sha256')}, holdout is {manifest['sha256']}). "
            "Its training data may overlap the holdout."
        )

    if list(candidate_metadata.get("feature_columns", [])) != list(manifest["feature_columns"]):
        problems.append("Candidate feature columns do not match the holdout's feature columns")

    return problems


def run(args) -> int:
    holdout_df, manifest = load_holdout(args.holdout_dir)
    feature_columns = manifest["feature_columns"]
    target_column = manifest["target_column"]

    candidate = load_model(args.candidate_uri)
    problems = check_candidate_matches_holdout(candidate.metadata, manifest)

    candidate_result = evaluate(
        candidate.model, holdout_df, feature_columns, target_column, args.segment_column
    )

    production_result = None
    production_version = None
    if args.production_uri:
        try:
            production = load_model(args.production_uri)
            production_version = production.version
            production_result = evaluate(
                production.model, holdout_df, feature_columns, target_column, args.segment_column
            )
        except Exception as error:
            if not args.allow_missing_production:
                raise
            print(f"No production model at {args.production_uri} ({error}); gating on absolute limits only")

    gate = EvaluationGate(
        rmse_tolerance=args.rmse_tolerance,
        segment_tolerance=args.segment_tolerance,
        min_segment_rows=args.min_segment_rows,
        max_rmse_without_baseline=args.max_rmse_without_baseline,
    )
    result = gate.check(candidate_result, production_result)
    result.reasons = problems + result.reasons
    result.passed = result.passed and not problems

    report = {
        **result.to_dict(),
        "holdout_version": manifest["version"],
        "holdout_sha256": manifest["sha256"],
        "candidate_uri": candidate.uri,
        "candidate_version": candidate.version,
        "production_version": production_version,
    }

    if args.report:
        with open(args.report, "w", encoding="utf-8") as report_file:
            json.dump(report, report_file, indent=4)

    print(json.dumps({"passed": report["passed"], "reasons": report["reasons"]}, indent=2))
    return 0 if result.passed else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Gate a candidate model against the frozen holdout")
    parser.add_argument("--holdout-dir", required=True)
    parser.add_argument("--candidate-uri", required=True)
    parser.add_argument("--production-uri")
    parser.add_argument("--allow-missing-production", action="store_true",
                        help="First deployment: no champion exists yet")
    parser.add_argument("--segment-column", default="conference_id")
    parser.add_argument("--rmse-tolerance", type=float, default=0.0)
    parser.add_argument("--segment-tolerance", type=float, default=0.15)
    parser.add_argument("--min-segment-rows", type=int, default=1)
    parser.add_argument("--max-rmse-without-baseline", type=float)
    parser.add_argument("--report")
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except Exception as error:
        print(f"Evaluation gate could not run: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
