from pathlib import Path
import re

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"

TOP_LEVEL_PACKAGES = [
    "DataIngestion",
    "FeatureEngineering",
    "Infrastructure",
    "MetricsAndMonitoring",
    "ModelTrainingAndDeployment",
]


def add_init_files(src_dir: Path) -> None:
    for directory in [src_dir] + [p for p in src_dir.rglob("*") if p.is_dir()]:
        init_file = directory / "__init__.py"
        if not init_file.exists():
            init_file.write_text("", encoding="utf-8")
            print(f"Created {init_file}")


def rewrite_imports_in_file(file_path: Path, packages: list[str]) -> bool:
    original = file_path.read_text(encoding="utf-8")
    updated = original

    for package in packages:
        from_pattern = re.compile(
            rf"(?m)^(\s*from\s+){package}(\.[A-Za-z0-9_\.]+\s+import\s+)"
        )
        import_pattern = re.compile(
            rf"(?m)^(\s*import\s+){package}(\.[A-Za-z0-9_\.]+)?(\s*(?:as\s+\w+)?\s*)$"
        )

        updated = from_pattern.sub(rf"\1src.{package}\2", updated)
        updated = import_pattern.sub(
            lambda m: f"{m.group(1)}src.{package}{m.group(2) or ''}{m.group(3)}",
            updated,
        )

    if updated != original:
        file_path.write_text(updated, encoding="utf-8")
        print(f"Updated imports in {file_path}")
        return True

    return False


def main() -> None:
    if not SRC_DIR.exists():
        raise FileNotFoundError(f"src directory not found: {SRC_DIR}")

    add_init_files(SRC_DIR)

    python_files = [
        p for p in PROJECT_ROOT.rglob("*.py")
        if ".venv" not in p.parts and "__pycache__" not in p.parts
    ]

    changed_count = 0
    for file_path in python_files:
        if rewrite_imports_in_file(file_path, TOP_LEVEL_PACKAGES):
            changed_count += 1

    print(f"\nDone. Updated {changed_count} Python files.")


if __name__ == "__main__":
    main()