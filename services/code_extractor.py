"""
Code Extractor Service — tree-sitter based code structure extraction.

Extracts from source code files:
- Function/method definitions (name, signature, docstring, line range)
- Class definitions (name, base classes, docstring)
- Import statements
- Top-level comments and module docstrings

Supports: Python, JavaScript, TypeScript, Rust, Go, Java, C/C++.
Falls back to regex for unsupported languages.
"""

import re
from dataclasses import dataclass, field
from typing import Optional
import structlog

logger = structlog.get_logger()


# ============================================================================
# DATA MODELS
# ============================================================================

@dataclass
class FunctionInfo:
    name: str
    signature: str = ""
    docstring: str = ""
    start_line: int = 0
    end_line: int = 0
    is_method: bool = False
    parent_class: str = ""


@dataclass
class ClassInfo:
    name: str
    bases: list = field(default_factory=list)
    docstring: str = ""
    methods: list = field(default_factory=list)   # list[FunctionInfo]
    start_line: int = 0
    end_line: int = 0


@dataclass
class CodeStructure:
    language: str = ""
    functions: list = field(default_factory=list)   # list[FunctionInfo]
    classes: list = field(default_factory=list)     # list[ClassInfo]
    imports: list = field(default_factory=list)     # list[str]
    docstring: str = ""                             # module-level docstring
    line_count: int = 0
    extractor: str = "tree-sitter"                  # or "regex"
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "language": self.language,
            "line_count": self.line_count,
            "extractor": self.extractor,
            "module_docstring": self.docstring,
            "imports": self.imports,
            "functions": [
                {
                    "name": f.name,
                    "signature": f.signature,
                    "docstring": f.docstring[:300],
                    "start_line": f.start_line,
                    "end_line": f.end_line,
                    "is_method": f.is_method,
                    "parent_class": f.parent_class,
                }
                for f in self.functions
            ],
            "classes": [
                {
                    "name": c.name,
                    "bases": c.bases,
                    "docstring": c.docstring[:300],
                    "method_count": len(c.methods),
                    "start_line": c.start_line,
                    "end_line": c.end_line,
                }
                for c in self.classes
            ],
        }

    def to_searchable_text(self) -> str:
        """Flatten structure to a searchable text blob."""
        parts = []
        if self.docstring:
            parts.append(self.docstring)
        for imp in self.imports:
            parts.append(imp)
        for cls in self.classes:
            parts.append(f"class {cls.name}")
            if cls.docstring:
                parts.append(cls.docstring)
            for m in cls.methods:
                parts.append(f"def {m.name}: {m.docstring}")
        for fn in self.functions:
            parts.append(f"def {fn.name}: {fn.docstring}")
        return "\n".join(parts)


# ============================================================================
# LANGUAGE CONFIGURATION
# ============================================================================

# Maps file extension → (tree-sitter grammar module, language name)
LANGUAGE_MAP = {
    ".py":   ("tree_sitter_python",     "python"),
    ".js":   ("tree_sitter_javascript", "javascript"),
    ".jsx":  ("tree_sitter_javascript", "javascript"),
    ".ts":   ("tree_sitter_typescript", "typescript"),
    ".tsx":  ("tree_sitter_typescript", "tsx"),
    ".rs":   ("tree_sitter_rust",       "rust"),
    ".go":   ("tree_sitter_go",         "go"),
    ".java": ("tree_sitter_java",       "java"),
    ".c":    ("tree_sitter_c",          "c"),
    ".cpp":  ("tree_sitter_cpp",        "cpp"),
    ".h":    ("tree_sitter_c",          "c"),
}

# Languages we can detect by extension name (informational)
READABLE_NAME = {
    "python": "Python", "javascript": "JavaScript", "typescript": "TypeScript",
    "tsx": "TypeScript/React", "rust": "Rust", "go": "Go",
    "java": "Java", "c": "C", "cpp": "C++",
}


# ============================================================================
# CODE EXTRACTOR
# ============================================================================

class CodeExtractor:
    """
    Extracts structural information from source code files.

    Uses tree-sitter when available; falls back to regex for unsupported
    languages or when tree-sitter is not installed.
    """

    def __init__(self):
        self._parsers: dict = {}   # cache: language → tree_sitter.Parser

    def extract(self, code: str, language_hint: str = "") -> CodeStructure:
        """
        Extract structure from source code.

        Args:
            code: Source code text
            language_hint: Language name or file extension (e.g. 'python', '.py')

        Returns:
            CodeStructure with functions, classes, imports, docstring
        """
        # Normalize language hint
        if language_hint.startswith("."):
            lang = LANGUAGE_MAP.get(language_hint.lower(), (None, language_hint[1:]))[1]
        else:
            lang = language_hint.lower()

        # Try tree-sitter first
        ts_result = self._try_tree_sitter(code, language_hint, lang)
        if ts_result:
            return ts_result

        # Fallback to regex-based extraction
        return self._extract_regex(code, lang)

    def extract_from_file(self, file_path: str) -> CodeStructure:
        """Extract from a file path (infers language from extension)."""
        import os
        ext = os.path.splitext(file_path)[1].lower()
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                code = f.read()
        except Exception as e:
            result = CodeStructure(error=f"Cannot read file: {e}")
            return result
        return self.extract(code, language_hint=ext)

    # -------------------------------------------------------------------------
    # tree-sitter extraction
    # -------------------------------------------------------------------------

    def _try_tree_sitter(self, code: str, lang_hint: str, lang: str) -> Optional[CodeStructure]:
        """Attempt tree-sitter parsing. Returns None if not available."""
        try:
            import tree_sitter  # noqa
            from tree_sitter import Language, Parser

            # Normalize to extension key for grammar lookup
            ext = f".{lang}" if not lang_hint.startswith(".") else lang_hint.lower()
            grammar_module, ts_lang = LANGUAGE_MAP.get(ext, (None, lang))

            if not grammar_module:
                return None

            # Get or build parser
            parser = self._get_parser(ts_lang, grammar_module)
            if not parser:
                return None

            tree = parser.parse(code.encode("utf-8"))
            root = tree.root_node

            lines = code.splitlines()
            structure = CodeStructure(
                language=READABLE_NAME.get(ts_lang, ts_lang),
                line_count=len(lines),
                extractor="tree-sitter",
            )

            # Language-specific extraction
            if ts_lang == "python":
                self._extract_python_ts(root, code, lines, structure)
            elif ts_lang in ("javascript", "typescript", "tsx"):
                self._extract_js_ts(root, code, lines, structure)
            elif ts_lang == "rust":
                self._extract_rust_ts(root, code, lines, structure)
            else:
                # Generic: just get named function/class nodes
                self._extract_generic_ts(root, code, lines, structure)

            return structure

        except ImportError:
            logger.debug("tree_sitter_not_installed")
            return None
        except Exception as e:
            logger.warning("tree_sitter_extraction_failed", error=str(e))
            return None

    def _get_parser(self, lang: str, grammar_module: str):
        """Get or create a tree-sitter parser for the given language."""
        if lang in self._parsers:
            return self._parsers[lang]
        try:
            from tree_sitter import Language, Parser
            mod = __import__(grammar_module)
            language = Language(mod.language())
            parser = Parser(language)
            self._parsers[lang] = parser
            return parser
        except Exception as e:
            logger.debug("parser_build_failed", lang=lang, error=str(e))
            return None

    def _node_text(self, node, code: str) -> str:
        return code[node.start_byte:node.end_byte]

    def _extract_python_ts(self, root, code: str, lines: list, structure: CodeStructure):
        """Extract Python-specific nodes."""
        for node in root.children:
            if node.type == "expression_statement":
                # Module docstring is first expr_stmt with string content
                for child in node.children:
                    if child.type in ("string", "concatenated_string"):
                        txt = self._node_text(child, code).strip("'\"").strip()
                        if not structure.docstring:
                            structure.docstring = txt
                        break

            elif node.type == "import_statement" or node.type == "import_from_statement":
                structure.imports.append(self._node_text(node, code).strip())

            elif node.type == "function_definition":
                fn = self._parse_python_function(node, code)
                structure.functions.append(fn)

            elif node.type == "class_definition":
                cls = self._parse_python_class(node, code)
                structure.classes.append(cls)

    def _parse_python_function(self, node, code: str, parent_class: str = "") -> FunctionInfo:
        name = ""
        signature = self._node_text(node, code).split(":")[0].strip()
        docstring = ""

        for child in node.children:
            if child.type == "identifier" and not name:
                name = self._node_text(child, code)
            elif child.type == "block":
                # First statement may be docstring
                for stmt in child.children:
                    if stmt.type == "expression_statement":
                        for expr in stmt.children:
                            if expr.type in ("string", "concatenated_string"):
                                docstring = self._node_text(expr, code).strip("'\"").strip()
                        break

        return FunctionInfo(
            name=name,
            signature=signature,
            docstring=docstring,
            start_line=node.start_point[0] + 1,
            end_line=node.end_point[0] + 1,
            is_method=bool(parent_class),
            parent_class=parent_class,
        )

    def _parse_python_class(self, node, code: str) -> ClassInfo:
        name = ""
        bases = []
        docstring = ""
        methods = []

        for child in node.children:
            if child.type == "identifier" and not name:
                name = self._node_text(child, code)
            elif child.type == "argument_list":
                for b in child.children:
                    if b.type in ("identifier", "attribute"):
                        bases.append(self._node_text(b, code))
            elif child.type == "block":
                for stmt in child.children:
                    if stmt.type == "expression_statement" and not docstring:
                        for expr in stmt.children:
                            if expr.type in ("string", "concatenated_string"):
                                docstring = self._node_text(expr, code).strip("'\"").strip()
                    elif stmt.type == "function_definition":
                        methods.append(self._parse_python_function(stmt, code, parent_class=name))

        return ClassInfo(
            name=name,
            bases=bases,
            docstring=docstring,
            methods=methods,
            start_line=node.start_point[0] + 1,
            end_line=node.end_point[0] + 1,
        )

    def _extract_js_ts(self, root, code: str, lines: list, structure: CodeStructure):
        """Extract JS/TS functions, classes, imports."""
        def walk(node):
            t = node.type
            if t in ("import_declaration", "import_statement"):
                structure.imports.append(self._node_text(node, code).strip())
            elif t in ("function_declaration", "method_definition", "arrow_function"):
                name_node = node.child_by_field_name("name")
                name = self._node_text(name_node, code) if name_node else "(anonymous)"
                structure.functions.append(FunctionInfo(
                    name=name,
                    signature=self._node_text(node, code)[:120].split("{")[0].strip(),
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                ))
            elif t == "class_declaration":
                name_node = node.child_by_field_name("name")
                name = self._node_text(name_node, code) if name_node else ""
                structure.classes.append(ClassInfo(
                    name=name,
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                ))
            for child in node.children:
                walk(child)
        walk(root)

    def _extract_rust_ts(self, root, code: str, lines: list, structure: CodeStructure):
        """Extract Rust fn, struct, impl, use statements."""
        def walk(node):
            t = node.type
            if t == "use_declaration":
                structure.imports.append(self._node_text(node, code).strip())
            elif t == "function_item":
                name_node = node.child_by_field_name("name")
                name = self._node_text(name_node, code) if name_node else ""
                sig = self._node_text(node, code)[:200].split("{")[0].strip()
                structure.functions.append(FunctionInfo(
                    name=name,
                    signature=sig,
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                ))
            elif t in ("struct_item", "enum_item", "impl_item"):
                name_node = node.child_by_field_name("name")
                name = self._node_text(name_node, code) if name_node else ""
                structure.classes.append(ClassInfo(
                    name=name,
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                ))
            for child in node.children:
                walk(child)
        walk(root)

    def _extract_generic_ts(self, root, code: str, lines: list, structure: CodeStructure):
        """Generic tree-sitter extraction by node type names."""
        FUNC_TYPES = {"function_definition", "function_declaration", "method_declaration",
                      "function_item", "method_definition"}
        CLASS_TYPES = {"class_definition", "class_declaration", "struct_item",
                       "interface_declaration", "impl_item"}

        def walk(node):
            if node.type in FUNC_TYPES:
                name_node = node.child_by_field_name("name")
                name = self._node_text(name_node, code) if name_node else ""
                structure.functions.append(FunctionInfo(
                    name=name,
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                ))
            elif node.type in CLASS_TYPES:
                name_node = node.child_by_field_name("name")
                name = self._node_text(name_node, code) if name_node else ""
                structure.classes.append(ClassInfo(
                    name=name,
                    start_line=node.start_point[0] + 1,
                    end_line=node.end_point[0] + 1,
                ))
            for child in node.children:
                walk(child)
        walk(root)

    # -------------------------------------------------------------------------
    # Regex fallback
    # -------------------------------------------------------------------------

    def _extract_regex(self, code: str, lang: str) -> CodeStructure:
        """Regex-based extraction for when tree-sitter is unavailable."""
        structure = CodeStructure(
            language=READABLE_NAME.get(lang, lang),
            line_count=len(code.splitlines()),
            extractor="regex",
        )

        if lang == "python":
            self._regex_python(code, structure)
        elif lang in ("javascript", "typescript"):
            self._regex_js(code, structure)
        elif lang == "rust":
            self._regex_rust(code, structure)
        else:
            self._regex_generic(code, structure)

        return structure

    def _regex_python(self, code: str, structure: CodeStructure):
        # Module docstring
        m = re.match(r'^\s*(?:\'\'\'|""")(.*?)(?:\'\'\'|""")', code, re.DOTALL)
        if m:
            structure.docstring = m.group(1).strip()

        # Imports
        for m in re.finditer(r"^(?:import|from)\s+.+$", code, re.MULTILINE):
            structure.imports.append(m.group().strip())

        # Functions
        for m in re.finditer(
            r"^def\s+(\w+)\s*\(([^)]*)\)\s*(?:->.*?)?:", code, re.MULTILINE
        ):
            structure.functions.append(FunctionInfo(
                name=m.group(1),
                signature=m.group(0).strip(),
                start_line=code[:m.start()].count("\n") + 1,
            ))

        # Classes
        for m in re.finditer(r"^class\s+(\w+)(\([^)]*\))?:", code, re.MULTILINE):
            bases = []
            if m.group(2):
                bases = [b.strip() for b in m.group(2).strip("()").split(",") if b.strip()]
            structure.classes.append(ClassInfo(
                name=m.group(1),
                bases=bases,
                start_line=code[:m.start()].count("\n") + 1,
            ))

    def _regex_js(self, code: str, structure: CodeStructure):
        for m in re.finditer(r"^import\s+.+$", code, re.MULTILINE):
            structure.imports.append(m.group().strip())
        for m in re.finditer(
            r"(?:function\s+(\w+)|const\s+(\w+)\s*=\s*(?:async\s*)?\()", code
        ):
            name = m.group(1) or m.group(2)
            structure.functions.append(FunctionInfo(name=name))
        for m in re.finditer(r"class\s+(\w+)", code):
            structure.classes.append(ClassInfo(name=m.group(1)))

    def _regex_rust(self, code: str, structure: CodeStructure):
        for m in re.finditer(r"^use\s+.+;", code, re.MULTILINE):
            structure.imports.append(m.group().strip())
        for m in re.finditer(r"(?:pub\s+)?fn\s+(\w+)\s*\(", code):
            structure.functions.append(FunctionInfo(name=m.group(1)))
        for m in re.finditer(r"(?:pub\s+)?(?:struct|enum|impl)\s+(\w+)", code):
            structure.classes.append(ClassInfo(name=m.group(1)))

    def _regex_generic(self, code: str, structure: CodeStructure):
        # Try to find function-like patterns across most languages
        for m in re.finditer(
            r"(?:function|def|fn|func|void|int|string)\s+(\w+)\s*\(", code
        ):
            structure.functions.append(FunctionInfo(name=m.group(1)))
        for m in re.finditer(r"(?:class|struct|interface|type)\s+(\w+)", code):
            structure.classes.append(ClassInfo(name=m.group(1)))


# Global singleton
code_extractor = CodeExtractor()
