"""Output writers for the RDD supplement.

Produces .tex to be pulled into the overleaf write up

This all written by claude


Two consumers:
1. `supplement.qmd` renders inline (Quarto → PDF for OSF).
2. `paper-outputs/` is the Overleaf sync target — the same numbers/tables/figures
   land there as .tex fragments the manuscript pulls in via \\input{} and
   \\newcommand{} macros.

Conventions:
- `write_macro(name, value)` → appends/updates `\\newcommand{\\name}{value}` in
  paper-outputs/macros.tex. Manuscript then writes `\\name` inline.
- `write_tabular(df, name, ...)` → paper-outputs/<name>.tex, a full `table`
  environment with booktabs. Manuscript does `\\input{paper-outputs/<name>}`.
- `save_figure(fig, name)` → paper-outputs/<name>.pdf. Manuscript does
  `\\includegraphics{paper-outputs/<name>}`.
"""

from pathlib import Path

import pandas as pd

OUT = Path(__file__).parent / "paper-outputs"
OUT.mkdir(exist_ok=True)
MACROS = OUT / "macros.tex"


def _fmt(value, digits=3):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return f"{value:,}".replace(",", "\\,")
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def write_macro(name: str, value, digits: int = 3) -> None:
    line = f"\\newcommand{{\\{name}}}{{{_fmt(value, digits)}}}"
    lines: list[str] = []
    replaced = False
    if MACROS.exists():
        for existing in MACROS.read_text().splitlines():
            if existing.startswith(f"\\newcommand{{\\{name}}}"):
                lines.append(line)
                replaced = True
            else:
                lines.append(existing)
    if not replaced:
        lines.append(line)
    MACROS.write_text("\n".join(lines) + "\n")


def write_tabular(
    df: pd.DataFrame,
    name: str,
    caption: str = "",
    label: str | None = None,
    precision: int = 3,
    column_format: str | None = None,
) -> Path:
    label = label or name
    styler = df.style.hide(axis="index").format(precision=precision)
    body = styler.to_latex(hrules=True, column_format=column_format)
    wrapped = (
        "\\begin{table}[htbp]\n"
        "\\centering\n"
        f"\\caption{{{caption}}}\n"
        f"\\label{{tab:{label}}}\n"
        f"{body}"
        "\\end{table}\n"
    )
    path = OUT / f"{name}.tex"
    path.write_text(wrapped)
    return path


def save_figure(fig, name: str, dpi: int = 200) -> Path:
    """Save a matplotlib Figure to paper-outputs/<name>.pdf."""
    path = OUT / f"{name}.pdf"
    fig.savefig(path, bbox_inches="tight", dpi=dpi)
    return path
