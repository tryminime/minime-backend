import os
import ast
import re

def get_imports(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            tree = ast.parse(f.read(), filename=filepath)
        except Exception:
            return set()
    
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                imports.add(n.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.level == 0:
                imports.add(node.module.split('.')[0])
    return imports

stdlib = {"os", "sys", "json", "time", "datetime", "uuid", "re", "math", "random", "logging", "asyncio", "typing", "collections", "itertools", "functools", "pathlib", "subprocess", "socket", "hashlib", "io", "copy", "tempfile", "shutil", "urllib", "base64", "csv", "sqlite3", "pdb", "traceback", "inspect", "ast", "warnings", "enum", "typing_extensions", "contextlib", "__future__", "importlib", "gc", "threading", "multiprocessing", "queue", "concurrent", "argparse", "optparse", "abc", "typing", "unittest", "mock", "dataclasses", "statistics", "secrets"}

all_imports = set()
for root, dirs, files in os.walk('.'):
    if 'venv' in root or '__pycache__' in root:
        continue
    for f in files:
        if f.endswith('.py'):
            path = os.path.join(root, f)
            all_imports.update(get_imports(path))

local_modules = []
for root, dirs, files in os.walk('.'):
    if 'venv' in root or '__pycache__' in root:
        continue
    for f in files:
        if f.endswith('.py') and f != '__init__.py':
            local_modules.append(f[:-3])
    for d in dirs:
        local_modules.append(d)

third_party = all_imports - stdlib - set(local_modules) - {"backend"}
print("Detected third party imports:")
for pkg in sorted(third_party):
    print(f"- {pkg}")
