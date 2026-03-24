from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


NULL_SENTINELS = {"__NULL__", "<null>", "None"}


def parse_context(argv: list[str]) -> dict[str, str | None]:
    context: dict[str, str | None] = {}
    index = 0
    while index < len(argv):
        token = argv[index]

        if token in {"--context_param", "--context_type"}:
            index += 1
            if index < len(argv) and "=" in argv[index]:
                key, value = argv[index].split("=", 1)
                if token == "--context_param":
                    context[key] = None if value in NULL_SENTINELS else value
            index += 1
            continue

        if token.startswith("--"):
            body = token[2:]
            if "=" in body:
                key, value = body.split("=", 1)
                context[key] = None if value in NULL_SENTINELS else value
                index += 1
                continue

            key = body
            if index + 1 < len(argv) and not argv[index + 1].startswith("--"):
                index += 1
                value = argv[index]
                context[key] = None if value in NULL_SENTINELS else value
            else:
                context[key] = "true"

        index += 1

    return context


def require(context: dict[str, str | None], key: str) -> str:
    value = context.get(key)
    if value is None or value == "":
        raise ValueError(f"Missing required context value: {key}")
    return value


def load_job_module(subfolder: str, filename: str):
    root = Path(__file__).resolve().parents[1]
    module_path = root / subfolder / f"{filename}.py"
    spec = importlib.util.spec_from_file_location(filename, module_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to load module from {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run(context: dict[str, str | None]) -> dict[str, str | int]:
    data_type = require(context, "W_DATA_TYPE").lower()
    if data_type == "file":
        module = load_job_module("s_ingestion_import_file_1_0_1", "S_INGESTION_IMPORT_FILE_1")
    else:
        module = load_job_module(
            "s_ingestion_import_tabella_0_1", "S_INGESTION_IMPORT_TABELLA"
        )

    result = module.run(context)
    result["selected_flow"] = "file" if data_type == "file" else "table"
    return result


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    context = parse_context(arguments)
    result = run(context)
    print(f"Selected flow: {result['selected_flow']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())