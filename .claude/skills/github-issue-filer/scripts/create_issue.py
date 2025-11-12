#!/usr/bin/env -S uv run --with pyyaml python
"""File AnyPod GitHub issues directly from the CLI."""

import argparse
from collections.abc import Iterable
from dataclasses import dataclass
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, cast

import yaml

DEFAULT_REPO = "thurstonsand/anypod"
TEMPLATE_MAP = {
    "bug": Path(".github/ISSUE_TEMPLATE/bug.yml"),
    "feature": Path(".github/ISSUE_TEMPLATE/feature.yml"),
}


def resolve_template_arg(arg: str) -> Path:
    """Return the template path for *arg* (bug/feature or explicit path)."""
    mapped = TEMPLATE_MAP.get(arg.lower())
    path = mapped or Path(arg)
    resolved = path.expanduser().resolve()
    if not resolved.exists():
        raise SystemExit(f"Template '{arg}' not found at {resolved}")
    return resolved


@dataclass
class Section:
    """Represent a single form block from a GitHub issue template."""

    identifier: str
    label: str
    optional: bool
    control_type: str
    render: str | None = None
    multiple: bool = False
    options: list[str] | None = None
    description: str | None = None

    def heading(self) -> str:
        """Return the heading label exactly as defined in the template."""
        return f"### {self.label}"


def parse_args() -> argparse.Namespace:
    """Return parsed CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Render and file AnyPod GitHub issues from YAML templates."
    )
    parser.add_argument(
        "template",
        help="Template shortcut ('bug' or 'feature') or path to a template YAML file",
    )
    parser.add_argument(
        "--data-file",
        type=Path,
        help="Path to JSON file with responses. Defaults to STDIN when omitted.",
    )
    parser.add_argument(
        "--summary",
        help="Summary text to drop into the template title placeholder.",
    )
    parser.add_argument(
        "--title",
        help=(
            "Override the final GitHub issue title. Otherwise the template title "
            "and --summary are combined."
        ),
    )
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Print JSON metadata about the template instead of creating an issue.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=argparse.SUPPRESS,  # internal use only
    )
    return parser.parse_args()


def load_template(template_path: Path) -> dict[str, Any]:
    """Load a GitHub issue template from *template_path*."""
    text = template_path.read_text(encoding="utf-8")
    return yaml.safe_load(text)


def build_sections(template: dict[str, Any]) -> list[Section]:
    """Extract Section objects from the template body blocks."""
    sections: list[Section] = []
    for block in template.get("body", []):
        control_type = block.get("type")
        if control_type not in {"textarea", "input", "dropdown"}:
            continue
        attributes: dict[str, Any] = block.get("attributes", {})
        validations: dict[str, Any] = block.get("validations", {})
        identifier = block.get("id")
        label = attributes.get("label")
        if not identifier or not label:
            raise ValueError("Template block missing id or label")
        sections.append(
            Section(
                identifier=identifier,
                label=label.strip(),
                optional=not bool(validations.get("required")),
                control_type=control_type,
                render=attributes.get("render"),
                multiple=bool(attributes.get("multiple", False)),
                options=attributes.get("options"),
                description=attributes.get("description"),
            )
        )
    return sections


def describe_sections(sections: list[Section]) -> str:
    """Return JSON describing *sections* keyed by identifier."""
    payload: dict[str, dict[str, Any]] = {}
    for section in sections:
        entry: dict[str, Any] = {
            "optional": section.optional,
            "type": section.control_type,
        }
        description = section.description or section.label
        if description:
            entry["description"] = description
        if section.options:
            entry["options"] = section.options
        if section.render:
            entry["render"] = section.render
        payload[section.identifier] = entry
    return json.dumps(payload, indent=2, sort_keys=True)


def load_responses(data_file: Path | None) -> dict[str, Any]:
    """Load user responses from *data_file* or STDIN when none is given."""
    if data_file is not None:
        raw = data_file.read_text(encoding="utf-8")
    else:
        raw = sys.stdin.read()
        if not raw.strip():
            raise ValueError("No response payload provided on STDIN")
    return json.loads(raw)


def format_dropdown(raw_value: Any, multiple: bool) -> str:
    """Format dropdown selections."""
    if isinstance(raw_value, str):
        values = [raw_value.strip()]
    elif isinstance(raw_value, (list, tuple, set)):
        iterable_value = cast(Iterable[Any], raw_value)
        values = [str(item).strip() for item in iterable_value]
    elif raw_value is None:
        values = []
    else:
        values = [str(raw_value).strip()]
    values = [value for value in values if value]
    if not values:
        return ""
    if multiple and len(values) > 1:
        return "\n".join(f"- {choice}" for choice in values)
    return values[0]


def format_value(section: Section, raw_value: Any) -> str:
    """Format *raw_value* according to *section* metadata."""
    if raw_value is None:
        return ""
    if section.control_type == "dropdown":
        return format_dropdown(raw_value, section.multiple)
    if section.control_type == "textarea":
        text_value = str(raw_value).rstrip()
        if not text_value.strip():
            return ""
        if section.render:
            return f"```{section.render}\n{text_value}\n```"
        return text_value
    text_value = str(raw_value).strip()
    return text_value


def render_body(sections: list[Section], responses: dict[str, Any]) -> str:
    """Return a Markdown body for *sections* using *responses*."""
    lines: list[str] = []
    for section in sections:
        formatted = format_value(section, responses.get(section.identifier))
        if not formatted.strip():
            if section.optional:
                continue
            raise ValueError(
                f"Missing response for required field '{section.identifier}'"
            )
        lines.append(section.heading())
        lines.append("")
        lines.append(formatted)
        lines.append("")
    if not lines:
        return ""
    return "\n".join(lines).strip() + "\n"


def resolve_title(
    template: dict[str, Any], explicit_title: str | None, summary: str | None
) -> str:
    """Derive the GitHub issue title."""
    if explicit_title:
        return explicit_title
    template_title = template.get("title", "").strip()
    if "<summary>" in template_title:
        if not summary:
            raise ValueError("Provide --summary or --title to build the issue title")
        return template_title.replace("<summary>", summary.strip())
    if template_title:
        return template_title
    if summary:
        return summary.strip()
    raise ValueError("Issue title is required via --title or --summary")


def resolve_labels(template: dict[str, Any]) -> list[str]:
    """Return labels declared in the template frontmatter."""
    labels_raw: Any = template.get("labels")
    if isinstance(labels_raw, str):
        cleaned = labels_raw.strip()
        return [cleaned] if cleaned else []
    if isinstance(labels_raw, list):
        list_values = cast(list[Any], labels_raw)
        labels: list[str] = []
        for raw_label in list_values:
            text = str(raw_label).strip()
            if text:
                labels.append(text)
        return labels
    return []


def publish_issue(
    title: str,
    labels: list[str],
    body: str,
    dry_run: bool,
) -> tuple[str | None, str]:
    """Create a GitHub issue via gh and return (url, body)."""
    gh_labels: list[str] = [
        str(label).strip() for label in labels if str(label).strip()
    ]
    repo = DEFAULT_REPO
    if dry_run:
        print("--- DRY RUN ---")
        print(f"Repo   : {repo}")
        print(f"Title  : {title}")
        print(f"Labels : {', '.join(gh_labels) if gh_labels else '<none>'}")
        print("Body:\n" + body)
        return None, body

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as tmp:
            tmp.write(body)
            tmp_path = Path(tmp.name)
        cmd = [
            "gh",
            "issue",
            "create",
            "--repo",
            repo,
            "--title",
            title,
            "--body-file",
            str(tmp_path),
        ]
        for label in gh_labels:
            cmd.extend(["--label", label])
        result = subprocess.run(
            cmd, check=True, capture_output=True, text=True
        )  # raises on error
        gh_output = result.stdout.strip()
        issue_url = None
        for line in reversed(gh_output.splitlines()):
            trimmed = line.strip()
            if trimmed.startswith(("http://", "https://")):
                issue_url = trimmed
                break
        if issue_url is None:
            raise RuntimeError(f"Unable to parse issue URL from gh output: {gh_output}")
        issue_number = issue_url.rstrip("/").split("/")[-1]
    finally:
        if tmp_path:
            tmp_path.unlink(missing_ok=True)

    verify = subprocess.run(
        [
            "gh",
            "issue",
            "view",
            issue_number,
            "--repo",
            repo,
            "--json",
            "url,body",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    verification = json.loads(verify.stdout)
    if verification.get("body") != body:
        print(
            "Warning: GitHub rendered body differs from local render", file=sys.stderr
        )
    return issue_url, verification.get("body", "")


def main() -> None:
    """CLI entrypoint."""
    args = parse_args()
    template_path = resolve_template_arg(args.template)
    template = load_template(template_path)
    sections = build_sections(template)

    if args.describe:
        print(describe_sections(sections))
        return

    responses = load_responses(args.data_file)
    body = render_body(sections, responses)
    title = resolve_title(template, args.title, args.summary)
    labels = resolve_labels(template)

    issue_url, _ = publish_issue(title, labels, body, args.dry_run)

    if issue_url:
        print(f"Issue created: {issue_url}")
    else:
        print("Dry run completed. No issue created.")


if __name__ == "__main__":
    main()
