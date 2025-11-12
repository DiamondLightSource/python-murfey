from __future__ import annotations

import argparse
import re
import textwrap

import yaml


class LineWrapHelpFormatter(argparse.RawDescriptionHelpFormatter):
    """
    A helper class for formatting the help messages the CLIs nicely. This implementation
    will preserve indents at the start of a line and interpret newline metacharacters
    accordingly.

    Credits: https://stackoverflow.com/a/35925919
    """

    def _add_whitespace(self, idx, wspace_idx, text):
        if idx == 0:
            return text
        return (" " * wspace_idx) + text

    def _split_lines(self, text, width):
        text_rows = text.splitlines()
        for idx, line in enumerate(text_rows):
            search = re.search(r"\s*[0-9\-]{0,}\.?\s*", line)
            if line.strip() == "":
                text_rows[idx] = " "
            elif search:
                wspace_line = search.end()
                lines = [
                    self._add_whitespace(i, wspace_line, x)
                    for i, x in enumerate(textwrap.wrap(line, width))
                ]
                text_rows[idx] = lines
        return [item for sublist in text_rows for item in sublist]


class PrettierDumper(yaml.Dumper):
    """
    Custom YAML Dumper class that sets `indentless` to False. This generates a YAML
    file that is then compliant with Prettier's formatting style
    """

    def increase_indent(self, flow=False, indentless=False):
        # Force 'indentless=False' so list items align with Prettier
        return super(PrettierDumper, self).increase_indent(flow, indentless=False)


def prettier_str_representer(dumper, data):
    """
    Helper function to format strings according to Prettier's standards:
    - No quoting unless it can be misinterpreted as another data type
    - When quoting, use double quotes unless string already contains double quotes
    """

    def is_implicitly_resolved(value: str) -> bool:
        for (
            first_char,
            resolvers,
        ) in yaml.resolver.Resolver.yaml_implicit_resolvers.items():
            if first_char is None or (value and value[0] in first_char):
                for resolver in resolvers:
                    if len(resolver) == 3:
                        _, regexp, _ = resolver
                    else:
                        _, regexp = resolver
                    if regexp.match(value):
                        return True
        return False

    # If no quoting is needed, use default plain style
    if not is_implicitly_resolved(data):
        return dumper.represent_scalar("tag:yaml.org,2002:str", data)

    # If the string already contains double quotes, fall back to single quotes
    if '"' in data and "'" not in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="'")

    # Otherwise, prefer double quotes
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style='"')


# Add the custom string representer to PrettierDumper
PrettierDumper.add_representer(str, prettier_str_representer)
