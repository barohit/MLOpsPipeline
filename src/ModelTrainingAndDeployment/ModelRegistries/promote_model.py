"""Move the registry aliases that the cluster serves from.

    # after a rollout finishes healthy
    python -m src.ModelTrainingAndDeployment.ModelRegistries.promote_model promote --version 7

    # undo: champion goes back to the version it replaced
    python -m src.ModelTrainingAndDeployment.ModelRegistries.promote_model rollback

MLFLOW_TRACKING_URI must point at the tracking server.
"""
import argparse
import sys

from src.ModelTrainingAndDeployment.ModelRegistries.MLFlowModelRegistry import (
    CHALLENGER_ALIAS,
    MLflowModelRegistry,
)
from src.ModelTrainingAndDeployment.feature_schema import MODEL_NAME


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Promote or roll back the served model version")
    parser.add_argument("action", choices=["promote", "rollback", "challenger"])
    parser.add_argument("--model-name", default=MODEL_NAME)
    parser.add_argument("--version")
    parser.add_argument("--tracking-uri")
    args = parser.parse_args(argv)

    registry = MLflowModelRegistry(tracking_uri=args.tracking_uri)

    if args.action in ("promote", "challenger") and not args.version:
        parser.error(f"{args.action} needs --version")

    if args.action == "promote":
        previous = registry.promote(args.model_name, args.version)
        print(f"champion: {previous} -> {args.version}")
    elif args.action == "challenger":
        registry.set_alias(args.model_name, CHALLENGER_ALIAS, args.version)
        print(f"challenger -> {args.version}")
    else:
        restored = registry.rollback(args.model_name)
        print(f"champion rolled back to {restored}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
