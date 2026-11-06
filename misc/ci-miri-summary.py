#!/usr/bin/env python3
"""Expose distinct Miri diagnostics when the full job log is unavailable."""

import hashlib
import re
import sys


MAX_DIAGNOSTICS = 20
MAX_ANNOTATION_CHARS = 1800
MAX_CONTEXT_LINES = 24
ANSI = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\))")
CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
MIRI_ERROR = re.compile(r"^error: (Undefined Behavior|unsupported operation)\b", re.I)
PANIC = re.compile(r"^(?:thread\b.*\bpanicked at\b|panicked at\b)")
ASSERTION = re.compile(r"^assertion\b.*\bfailed\b")
TEST_RESULT = re.compile(r"^\s*(?:FAIL|ABORT|TIMEOUT)\s+\[.*?\]\s+(?:\(\s*\d+\s*/\s*\d+\s*\)\s+)?(.+)")
LIBTEST_PREFIX = re.compile(r"^\s*test[ \t]+(\S+)[ \t]+\.\.\.[ \t]+")
SOURCE = re.compile(r"(?:-->\s*|\bpanicked at\s+)([^\r\n]*?:\d+(?::\d+)?)")


def reports(log):
    current = None
    test = ""
    for raw in log:
        line = CONTROL.sub("", ANSI.sub("", raw))
        result = TEST_RESULT.match(line)
        libtest = LIBTEST_PREFIX.match(line)
        if result or libtest:
            if current:
                yield current
                current = None
            test = (result or libtest).group(1).strip()
            if libtest:
                # Libtest may print the first diagnostic immediately after its test label.
                line = line[libtest.end():]

        stripped = line.lstrip()
        error = MIRI_ERROR.match(stripped)
        if error:
            kind = error.group(1).lower()
        elif PANIC.match(stripped):
            kind = "panic"
        elif ASSERTION.match(stripped):
            kind = "assertion"
        else:
            kind = None
        # An assertion body belongs to the preceding panic, rather than a second report.
        if kind == "assertion" and current and current["kind"] == "panic":
            kind = None
        if kind:
            if current:
                yield current
            current = {"kind": kind, "test": test, "lines": [line]}
        elif current:
            current["lines"].append(line)

        if current and len(current["lines"]) >= MAX_CONTEXT_LINES:
            yield current
            current = None
    if current:
        yield current


def report_key(report):
    body = "".join(report["lines"])
    location = SOURCE.search(body)
    # Thread names and Miri allocation/tag IDs vary between tests and runs.
    reason = re.sub(r"^thread\s+['\"].*?['\"](?:\s+\(\d+\))?", "thread '<thread>'", report["lines"][0].strip())
    if report["kind"] == "panic":
        # The panic header may contain only a location; include its actual reason.
        reason += " " + next((line.strip() for line in report["lines"][1:] if line.strip()), "")
    reason = re.sub(r"0x[0-9a-fA-F]+|\balloc\d+\b|<\d+>", "<id>", reason)
    key = (report["kind"], reason, location.group(1) if location else "")
    return hashlib.sha256(repr(key).encode()).digest()


def escaped_excerpt(text, limit):
    """Bound the complete encoded message without splitting a workflow escape."""
    chunks = []
    length = 0
    suffix = "...[truncated]"
    for char in text:
        chunk = {"%": "%25", "\r": "%0D", "\n": "%0A"}.get(char, char)
        if length + len(chunk) > limit - len(suffix):
            return "".join(chunks) + suffix
        chunks.append(chunk)
        length += len(chunk)
    return "".join(chunks)


def summarize(log):
    seen = set()
    count = 0
    shown = 0
    for report in reports(log):
        count += 1
        key = report_key(report)
        if key in seen:
            continue
        seen.add(key)
        if shown >= MAX_DIAGNOSTICS:
            continue
        shown += 1
        prefix = f"::error title=Miri diagnostic {shown}: {report['kind']}::"
        body = "".join(report["lines"])
        source = SOURCE.search(body)
        context = f"Location: {source.group(1)}\n" if source else ""
        context += f"Test: {report['test']}\n" if report["test"] else ""
        context += body
        print(prefix + escaped_excerpt(context, MAX_ANNOTATION_CHARS - len(prefix)))
    print(f"Miri diagnostics: {count} reports, {len(seen)} distinct causes; "
          f"showing {shown} (limit {MAX_DIAGNOSTICS}).")


def main():
    if len(sys.argv) != 2:
        print("Usage: ci-miri-summary.py LOG_FILE", file=sys.stderr)
        return 64
    try:
        # Keep CRLF intact so all workflow command data is escaped explicitly.
        with open(sys.argv[1], encoding="utf-8", errors="replace", newline="") as log:
            summarize(log)
    except OSError as error:
        print(f"Could not read Miri diagnostic log: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
