from __future__ import annotations

import functools
import time
from datetime import datetime

from rich.align import Align
from rich.box import SQUARE
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.table import Column
from textual.reactive import Reactive
from textual.widget import Widget

from murfey.client.tui.progress import BlockBarColumn


class StatusBar(Widget):
    transferred = Reactive([0, 0])

    @functools.lru_cache()
    def get_progress(self):
        text_column = TextColumn("{task.description}", table_column=Column(ratio=1))
        bar_column = BlockBarColumn(bar_width=None, table_column=Column(ratio=3))
        progress = Progress(
            text_column,
            bar_column,
            TransferSpeedColumn(),
            SpinnerColumn(),
            TimeRemainingColumn(),
            expand=True,
        )

        task1 = progress.add_task("[red]Downloading...", total=self.transferred[1])
        task2 = progress.add_task("[green]Processing...", total=1000)
        task3 = progress.add_task("[cyan]Cooking...", total=1000)
        return (progress, task1, task2, task3)

    def render(self) -> Panel:
        progress, task1, task2, task3 = self.get_progress()
        elapsed = (time.time() - self.start) * 100

        progress.update(task1, completed=self.transferred[0], total=self.transferred[1])
        progress.update(task2, completed=max(0, min(1000, elapsed - 1000)))
        progress.update(task3, completed=max(0, min(1000, elapsed - 2000)))
        return Panel(progress.make_tasks_table(progress.tasks), height=5, box=SQUARE)

        timestamp = datetime.now().strftime("%c")
        return Align.center(timestamp, vertical="middle")
        return Panel("Hello [b]World[/b]", style="", height=3, box=SQUARE)

    def on_mount(self):
        self.start = time.time()
        self.set_interval(0.03, self.tick)

    def tick(self):
        self.refresh()
