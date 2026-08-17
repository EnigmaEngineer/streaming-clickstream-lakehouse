"""Checks that read source rather than run it.

`stream.job.run` takes an argparse namespace and reads attributes off it. Two scripts
build that namespace by hand with `SimpleNamespace`, so adding an option to the job is
a change that silently breaks every hand built caller. That happened on day 4, when
`--sink` arrived and `scripts/watermark_sweep.py` was left raising AttributeError on
every run for a day. Nothing caught it because nothing in the suite executes a script.

The detector is a function over a path, not an assertion inside a test. The 08-10
lesson on the previous project was that a check written inline cannot be tested from
inside itself, and its own mutant survives. Pointing it at fixture files with the
defect marked in them is what makes its behaviour a passing test rather than something
somebody once tried by hand.
"""

import ast
from pathlib import Path


def args_attributes_read(path, func_name: str = "run", param: str | None = None) -> set:
    """Every attribute read off the first parameter of `func_name` in `path`.

    Loads only. An attribute being assigned is the function adding a field rather than
    requiring one, and treating those as requirements would demand that callers
    pre-populate outputs.
    """
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == func_name:
            if not node.args.args:
                raise ValueError(f"{func_name} in {path} takes no arguments")
            name = param or node.args.args[0].arg
            found = set()
            for sub in ast.walk(node):
                if (
                    isinstance(sub, ast.Attribute)
                    and isinstance(sub.value, ast.Name)
                    and sub.value.id == name
                    and isinstance(sub.ctx, ast.Load)
                ):
                    found.add(sub.attr)
            return found
    raise ValueError(f"no function named {func_name} in {path}")


def namespace_calls(path) -> list:
    """Every SimpleNamespace construction in `path`, as sets of keyword names.

    Matches the bare name and the dotted form, because `types.SimpleNamespace(...)`
    and `SimpleNamespace(...)` are the same call and a detector that only knew one
    would report clean on a file using the other.
    """
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        fn = node.func
        name = None
        if isinstance(fn, ast.Name):
            name = fn.id
        elif isinstance(fn, ast.Attribute):
            name = fn.attr
        if name != "SimpleNamespace":
            continue
        out.append({kw.arg for kw in node.keywords if kw.arg is not None})
    return out


def missing_namespace_fields(job_path, script_paths, func_name: str = "run") -> dict:
    """Which hand built namespaces are missing an attribute the job reads.

    Returns a mapping of path to the sorted missing names. An empty mapping means
    every caller is complete.

    Raises when there is nothing to check. A gate that can pass on zero inputs will
    eventually be pointed at zero inputs and nobody will notice, which is the 08-04
    finding on the previous project and has since turned up in three separate tools
    here. "Nothing to check" is a finding.
    """
    required = args_attributes_read(job_path, func_name)
    if not required:
        raise ValueError(f"{func_name} in {job_path} reads no attributes, nothing to require")
    paths = [Path(p) for p in script_paths]
    if not paths:
        raise ValueError("no scripts given, which would report success having checked nothing")

    out = {}
    checked = 0
    for path in paths:
        for keywords in namespace_calls(path):
            checked += 1
            gap = required - keywords
            if gap:
                out[str(path)] = sorted(gap)
    if checked == 0:
        raise ValueError(f"no SimpleNamespace calls found across {len(paths)} files, nothing checked")
    return out
