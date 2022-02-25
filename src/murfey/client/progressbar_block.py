from __future__ import annotations

import math
from functools import lru_cache
from typing import List

import rich.progress_bar
from rich.color import Color, blend_rgb
from rich.color_triplet import ColorTriplet
from rich.console import Console, ConsoleOptions, RenderResult
from rich.segment import Segment
from rich.style import Style

PULSE_SIZE = rich.progress_bar.PULSE_SIZE

PROGRESS_BAR_BLOCKS = [
    " ",
    "▏",
    "▎",
    "▍",
    "▌",
    "▋",
    "▊",
    "▉",
    "█",
]

PROGRESS_BAR_BLOCKS = [
    " ",
    " ",
    "▌",
    "▌",
    "▌",
    "▌",
    "█",
    "█",
    "█",
]

RIGHT_BAR_BLOCKS = [
    "█",
    "▐",
    "▐",
    "▐",
    "▐",
    "▐",
    "▐",
    "▕",
    " ",
]

RIGHT_BAR_BLOCKS = [
    "█",
    "█",
    "▐",
    "▐",
    "▐",
    "▐",
    #    "▕",
    #    "▕",
    " ",
    " ",
    " ",
]

Shade = "░▒▓"


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

        return

        if ascii:
            bar = "-"
            complete_halves = (
                int(width * 2 * completed / self.total) if self.total else width * 2
            )
            bar_count = complete_halves // 2
            half_bar_count = complete_halves % 2
            style = console.get_style(self.style)
            complete_style = console.get_style(
                self.complete_style
                if self.completed < self.total
                else self.finished_style
            )
            _Segment = Segment
            if bar_count:
                yield _Segment("-" * bar_count, complete_style)
            if half_bar_count:
                yield _Segment(" " * half_bar_count, complete_style)

            if not console.no_color:
                remaining_bars = width - bar_count - half_bar_count
                if remaining_bars and console.color_system is not None:
                    if not half_bar_count and bar_count:
                        yield _Segment(" ", style)
                        remaining_bars -= 1
                    if remaining_bars:
                        yield _Segment(bar * remaining_bars, style)

        bar = "-" if ascii else PROGRESS_BAR_BLOCKS[-1]
        half_bar_left = " " if ascii else "╺"
        complete_eights = (
            int(width * 8 * completed / self.total) if self.total else width * 8
        )
        bar_count = complete_eights // 8
        half_bar_count = complete_eights % 8
        style = console.get_style(self.style)
        complete_style = console.get_style(
            self.complete_style if self.completed < self.total else self.finished_style
        )
        _Segment = Segment
        if bar_count:
            yield _Segment(bar * bar_count, complete_style)
        if half_bar_count:
            yield _Segment(PROGRESS_BAR_BLOCKS[half_bar_count], complete_style)

        if not console.no_color:
            remaining_bars = width - bar_count - min(1, half_bar_count)
            if remaining_bars and console.color_system is not None:
                if not half_bar_count:
                    yield _Segment(" ", style)
                    remaining_bars -= 1
                if remaining_bars:
                    yield _Segment(RIGHT_BAR_BLOCKS[half_bar_count], style)
                    remaining_bars -= 1
                #   for n, blockchar in enumerate(Shade):
                #     multiplier = 6
                #     if half_bar_count < n:
                #       multiplier = 5
                #     if remaining_bars and console.color_system is not None:
                #       yield _Segment((blockchar * multiplier)[:remaining_bars], style)
                #       remaining_bars -= min(multiplier, remaining_bars)
                if remaining_bars and console.color_system is not None:
                    yield _Segment(bar * remaining_bars, style)


def _bar(done, total, width=10):
    #  if done >= total:
    #    return PROGRESS_BAR_BLOCKS[-1]  * width

    done_bar = (done / total) * width
    done_blocks = int(done_bar)
    done_blocks_fraction = done_bar - done_blocks
    partial_block = int(done_blocks_fraction * (len(PROGRESS_BAR_BLOCKS) - 1))
    progress_bar = (
        PROGRESS_BAR_BLOCKS[-1] * int(done_blocks) + PROGRESS_BAR_BLOCKS[partial_block]
    )
    return (
        progress_bar.ljust(width, PROGRESS_BAR_BLOCKS[0])
        + f"  {done_blocks}  {partial_block}  ({done_bar})"
    )


if __name__ == "__main__":  # pragma: no cover
    console = Console()

    steps = 400
    bars = [
        BlockProgressBar(width=50, total=steps),
        rich.progress_bar.ProgressBar(width=50, total=steps),
        BlockProgressBar(width=50, pulse=True),
        rich.progress_bar.ProgressBar(width=50, pulse=True),
    ]

    import time

    console.show_cursor(False)
    for n in range(0, steps + 1, 1):
        for bar in bars:
            bar.update(n)
            console.print(bar)
            console.print("  ", end="")
        #     console.file.write("\n")
        console.file.write("\r")
        time.sleep(3 / steps)
    console.show_cursor(True)
    console.print()
