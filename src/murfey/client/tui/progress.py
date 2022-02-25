"""
A progressbar widget that uses block characters rather than a thin
horizontal line. This makes it more visible on Windows.
"""

from __future__ import annotations

import math
from functools import lru_cache
from typing import List

import rich.progress
import rich.progress_bar
from rich.color import Color, blend_rgb
from rich.color_triplet import ColorTriplet
from rich.console import Console, ConsoleOptions, RenderResult
from rich.segment import Segment
from rich.style import Style

PULSE_SIZE = rich.progress_bar.PULSE_SIZE


class BlockProgressBar(rich.progress_bar.ProgressBar):
    @lru_cache(maxsize=16)
    def _get_pulse_segments(
        self,
        fore_style: Style,
        back_style: Style,
        color_system: str,
        no_color: bool,
        ascii: bool = False,
    ) -> List[Segment]:
        """Get a list of segments to render a pulse animation.

        Returns:
            List[Segment]: A list of segments, one segment per character.
        """
        bar = "-" if ascii else "█"
        segments: List[Segment] = []
        if color_system not in ("standard", "eight_bit", "truecolor") or no_color:
            segments += [Segment(bar, fore_style)] * (PULSE_SIZE // 2)
            segments += [Segment(" " if no_color else bar, back_style)] * (
                PULSE_SIZE - (PULSE_SIZE // 2)
            )
            return segments

        append = segments.append
        fore_color = (
            fore_style.color.get_truecolor()
            if fore_style.color
            else ColorTriplet(255, 0, 255)
        )
        back_color = (
            back_style.color.get_truecolor()
            if back_style.color
            else ColorTriplet(0, 0, 0)
        )
        cos = math.cos
        pi = math.pi
        _Segment = Segment
        _Style = Style
        from_triplet = Color.from_triplet

        for index in range(PULSE_SIZE):
            position = index / PULSE_SIZE
            fade = 0.5 + cos((position * pi * 2)) / 2.0
            color = blend_rgb(fore_color, back_color, cross_fade=fade)
            append(_Segment(bar, _Style(color=from_triplet(color))))
        return segments

    def __rich_console__(
        self, console: Console, options: ConsoleOptions
    ) -> RenderResult:

        width = min(self.width or options.max_width, options.max_width)
        ascii = options.ascii_only
        if self.pulse:
            yield from self._render_pulse(console, width, ascii=ascii)
            return

        completed = min(self.total, max(0, self.completed))

        bar = "-" if ascii else "█"
        half_bar_right = " " if ascii else "▌"
        half_bar_left = " " if ascii else "▐"
        complete_halves = (
            int(width * 2 * completed / self.total) if self.total else width * 2
        )
        bar_count = complete_halves // 2
        half_bar_count = complete_halves % 2
        style = console.get_style(self.style)
        complete_style = console.get_style(
            self.complete_style if self.completed < self.total else self.finished_style
        )
        _Segment = Segment
        if bar_count:
            yield _Segment(bar * bar_count, complete_style)
        if half_bar_count:
            yield _Segment(half_bar_right * half_bar_count, complete_style)

        if not console.no_color:
            remaining_bars = width - bar_count - half_bar_count
            if remaining_bars and console.color_system is not None:
                if not half_bar_count and bar_count:
                    yield _Segment(half_bar_left, style)
                    remaining_bars -= 1
                if remaining_bars:
                    yield _Segment(bar * remaining_bars, style)


class BlockBarColumn(rich.progress.BarColumn):
    def render(self, task: rich.progress.Task) -> rich.progress_bar.ProgressBar:
        """Gets a progress bar widget for a task."""
        return BlockProgressBar(
            total=max(0, task.total),
            completed=max(0, task.completed),
            width=None if self.bar_width is None else max(1, self.bar_width),
            pulse=not task.started,
            animation_time=task.get_time(),
            style=self.style,
            complete_style=self.complete_style,
            finished_style=self.finished_style,
            pulse_style=self.pulse_style,
        )


if __name__ == "__main__":  # pragma: no cover
    from rich.theme import Theme

    custom_theme = Theme(
        {
            "bar.complete": "rgb(249,38,249)",
            "bar.finished": "rgb(31,156,31)",
            "bar.pulse": "rgb(249,38,249)",
        }
    )
    console = Console(theme=custom_theme)
    steps = 400
    arguments = {
        "width": 50,
    }
    bars = [
        BlockProgressBar(**arguments, total=steps),
        rich.progress_bar.ProgressBar(**arguments, total=steps),
        BlockProgressBar(**arguments, pulse=True),
        rich.progress_bar.ProgressBar(**arguments, pulse=True),
    ]

    import time

    console.show_cursor(False)
    for n in range(0, steps + 1, 1):
        for bar in bars:
            bar.update(n)
            console.print(bar)
            console.print(" ", end="")
        console.file.write("\r")
        time.sleep(3 / steps)
    console.show_cursor(True)
    console.print()
