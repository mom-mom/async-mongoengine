#!/usr/bin/env python3
"""Convert test_queryset.py from sync to async using libcst."""

import sys
import libcst as cst
import libcst.matchers as m


# Methods that need await when called on objects
ASYNC_METHODS = {
    "save", "delete", "reload", "update", "modify", "cascade_save",
    "drop_collection", "ensure_indexes", "create_index", "_get_collection",
    "get", "first", "create", "count",
    "insert", "in_bulk", "distinct", "sum", "average",
    "item_frequencies", "with_id", "upsert_one", "update_one",
    "exec_js", "map_reduce", "aggregate", "explain", "to_json",
    "select_related", "using",
}

# Standalone functions that need await
ASYNC_STANDALONE_FUNCS = {
    "disconnect", "disconnect_all", "get_as_pymongo",
}

# Raw pymongo methods that need await
PYMONGO_ASYNC_METHODS = {
    "find_one", "insert_one", "count_documents", "drop", "command",
    "index_information",
}

# Context managers that need async with
ASYNC_CONTEXT_MANAGERS = {"query_counter", "switch_db"}

# Common queryset variable names (heuristic)
QS_VAR_NAMES = {
    "people", "qs", "queryset", "results", "users", "docs", "query",
    "obj", "cursor", "returned_comments", "only_age",
    "people1", "people2", "people3", "qs1", "qs2", "qs_disk",
    "posts", "numbers",
}


def _get_terminal_method_name(node: cst.Call) -> str | None:
    """Get the method name from x.method()"""
    if isinstance(node.func, cst.Attribute) and isinstance(node.func.attr, cst.Name):
        return node.func.attr.value
    return None


def _node_has_objects(node) -> bool:
    """Check if a CST node contains '.objects' somewhere in the chain."""
    if isinstance(node, cst.Attribute):
        if isinstance(node.attr, cst.Name) and node.attr.value == "objects":
            return True
        return _node_has_objects(node.value)
    if isinstance(node, cst.Call):
        return _node_has_objects(node.func)
    if isinstance(node, cst.Subscript):
        return _node_has_objects(node.value)
    return False


def _is_qs_var(node) -> bool:
    """Check if node is a Name that looks like a queryset variable."""
    return isinstance(node, cst.Name) and node.value in QS_VAR_NAMES


def _is_queryset_like(node) -> bool:
    """Heuristic: does this expression look like a queryset?"""
    if _node_has_objects(node):
        return True
    if isinstance(node, cst.Call) and _node_has_objects(node):
        return True
    if _is_qs_var(node):
        return True
    return False


def _should_await_method(method_name: str) -> bool:
    """Check if this method name needs await."""
    return method_name in ASYNC_METHODS or method_name in PYMONGO_ASYNC_METHODS


def _maybe_await_call(node: cst.Call) -> cst.BaseExpression:
    """Wrap a call in Await if it's an async method call."""
    method_name = _get_terminal_method_name(node)

    if method_name is None:
        # Standalone function call
        if isinstance(node.func, cst.Name):
            if node.func.value in ASYNC_STANDALONE_FUNCS:
                return cst.Await(expression=node)
        return node

    # self.assertSequence call
    if method_name == "assertSequence":
        if isinstance(node.func, cst.Attribute) and isinstance(
            node.func.value, cst.Name
        ) and node.func.value.value == "self":
            return cst.Await(expression=node)

    if _should_await_method(method_name):
        return cst.Await(expression=node)

    return node


class AsyncTransformer(cst.CSTTransformer):
    def __init__(self):
        super().__init__()
        self._in_class = False

    # ── Class definition ──────────────────────────────────────────────
    def visit_ClassDef(self, node: cst.ClassDef) -> bool:
        if node.name.value == "TestQueryset":
            self._in_class = True
        return True

    def leave_ClassDef(self, original_node, updated_node):
        if original_node.name.value == "TestQueryset":
            self._in_class = False
            # Remove unittest.TestCase base class
            new_bases = []
            for base in updated_node.bases:
                try:
                    base_code = cst.parse_module("").code_for_node(base.value)
                except Exception:
                    base_code = ""
                if "unittest.TestCase" not in base_code:
                    new_bases.append(base)
            if not new_bases:
                updated_node = updated_node.with_changes(
                    bases=[],
                    lpar=cst.MaybeSentinel.DEFAULT,
                    rpar=cst.MaybeSentinel.DEFAULT,
                )
            else:
                updated_node = updated_node.with_changes(bases=new_bases)
        return updated_node

    # ── Function definitions ──────────────────────────────────────────
    def leave_FunctionDef(self, original_node, updated_node):
        if not self._in_class:
            return updated_node

        name = updated_node.name.value

        if name == "setUp":
            return updated_node.with_changes(
                name=cst.Name("setup_method"),
                asynchronous=cst.Asynchronous(),
            )
        elif name == "tearDown":
            return updated_node.with_changes(
                name=cst.Name("teardown_method"),
                asynchronous=cst.Asynchronous(),
            )
        elif name == "setUpClass":
            return updated_node.with_changes(name=cst.Name("setup_class"))
        elif name == "tearDownClass":
            return updated_node.with_changes(name=cst.Name("teardown_class"))
        elif name.startswith("test_") or name == "assertSequence":
            return updated_node.with_changes(asynchronous=cst.Asynchronous())

        return updated_node

    # ── Attribute: wrap Await in parens if it's the value ──────────────
    def leave_Attribute(self, original_node, updated_node):
        if not self._in_class:
            return updated_node
        # If we have `(await X.first()).delete`, the Await needs parens
        if isinstance(updated_node.value, cst.Await):
            # Wrap the Await in parentheses so it renders as (await ...).attr
            wrapped = updated_node.value.with_changes(
                lpar=[cst.LeftParen()],
                rpar=[cst.RightParen()],
            )
            return updated_node.with_changes(value=wrapped)
        return updated_node

    # ── With statements -> async with ─────────────────────────────────
    def leave_With(self, original_node, updated_node):
        if not self._in_class:
            return updated_node

        for item in updated_node.items:
            ctx_expr = item.item
            if isinstance(ctx_expr, cst.Call):
                func = ctx_expr.func
                if isinstance(func, cst.Name) and func.value in ASYNC_CONTEXT_MANAGERS:
                    return updated_node.with_changes(asynchronous=cst.Asynchronous())
        return updated_node

    # ── For statements -> async for ───────────────────────────────────
    def leave_For(self, original_node, updated_node):
        if not self._in_class:
            return updated_node

        iter_expr = updated_node.iter
        if _is_queryset_like(iter_expr):
            return updated_node.with_changes(asynchronous=cst.Asynchronous())
        return updated_node

    # ── List comprehension iterables -> async for ──────────────────────
    def leave_CompFor(self, original_node, updated_node):
        if not self._in_class:
            return updated_node
        if not updated_node.asynchronous:
            if _is_queryset_like(updated_node.iter):
                return updated_node.with_changes(asynchronous=cst.Asynchronous())
        return updated_node

    # ── Calls: add await, convert list()/next()/len() ─────────────────
    def leave_Call(self, original_node, updated_node):
        if not self._in_class:
            return updated_node

        # list(qs) -> [doc async for doc in qs]
        if isinstance(updated_node.func, cst.Name) and updated_node.func.value == "list":
            if len(updated_node.args) == 1:
                arg = updated_node.args[0].value
                if _is_queryset_like(arg):
                    return cst.ListComp(
                        elt=cst.Name("doc"),
                        for_in=cst.CompFor(
                            target=cst.Name("doc"),
                            iter=arg,
                            asynchronous=cst.Asynchronous(),
                        ),
                    )

        # next(qs) -> await qs.__anext__()
        if isinstance(updated_node.func, cst.Name) and updated_node.func.value == "next":
            if len(updated_node.args) == 1:
                arg = updated_node.args[0].value
                if _is_queryset_like(arg):
                    return cst.Await(
                        expression=cst.Call(
                            func=cst.Attribute(value=arg, attr=cst.Name("__anext__")),
                            args=[],
                        )
                    )

        # Await async method calls
        method_name = _get_terminal_method_name(updated_node)

        if method_name is None:
            if isinstance(updated_node.func, cst.Name):
                if updated_node.func.value in ASYNC_STANDALONE_FUNCS:
                    return cst.Await(expression=updated_node)
            return updated_node

        if method_name == "assertSequence":
            if (isinstance(updated_node.func, cst.Attribute)
                and isinstance(updated_node.func.value, cst.Name)
                and updated_node.func.value.value == "self"):
                return cst.Await(expression=updated_node)

        if _should_await_method(method_name):
            return cst.Await(expression=updated_node)

        return updated_node

    # ── Remove import unittest, add MongoDBTestCase ───────────────────
    def leave_ImportFrom(self, original_node, updated_node):
        try:
            module_code = cst.parse_module("").code_for_node(updated_node.module)
        except Exception:
            return updated_node

        if module_code == "tests.utils":
            names = updated_node.names
            if isinstance(names, (list, tuple)):
                has_it = any(
                    isinstance(n, cst.ImportAlias)
                    and isinstance(n.name, cst.Name)
                    and n.name.value == "MongoDBTestCase"
                    for n in names
                )
                if not has_it:
                    new_names = list(names) + [
                        cst.ImportAlias(
                            name=cst.Name("MongoDBTestCase"),
                            comma=cst.MaybeSentinel.DEFAULT,
                        )
                    ]
                    updated_node = updated_node.with_changes(names=new_names)
        return updated_node

    def leave_SimpleStatementLine(self, original_node, updated_node):
        # Remove `import unittest`
        for stmt in updated_node.body:
            if isinstance(stmt, cst.Import):
                if isinstance(stmt.names, (list, tuple)):
                    for alias in stmt.names:
                        if isinstance(alias.name, cst.Name) and alias.name.value == "unittest":
                            return cst.RemovalSentinel.REMOVE
        return updated_node

    def leave_If(self, original_node, updated_node):
        """Remove if __name__ == '__main__': unittest.main()"""
        if not self._in_class:
            test = updated_node.test
            if isinstance(test, cst.Comparison):
                left = test.left
                if isinstance(left, cst.Name) and left.value == "__name__":
                    return cst.RemovalSentinel.REMOVE
        return updated_node


class RemoveDoubleAwait(cst.CSTTransformer):
    """Remove nested awaits: await (await x) -> await x"""

    def leave_Await(self, original_node, updated_node):
        if isinstance(updated_node.expression, cst.Await):
            return updated_node.expression
        return updated_node


def main():
    filepath = sys.argv[1]
    with open(filepath, "r") as f:
        source = f.read()

    tree = cst.parse_module(source)

    print("Pass 1: Main async transformations...")
    tree = tree.visit(AsyncTransformer())

    print("Pass 2: Remove double awaits...")
    tree = tree.visit(RemoveDoubleAwait())

    result = tree.code
    with open(filepath, "w") as f:
        f.write(result)

    print(f"Done! Wrote {filepath}")


if __name__ == "__main__":
    main()
