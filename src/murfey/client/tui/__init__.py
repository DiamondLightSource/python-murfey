from __future__ import annotations

import functools
import string
import time
from datetime import datetime
from typing import Union

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
from textual import events
from textual.app import App
from textual.keys import Keys
from textual.reactive import Reactive
from textual.widget import Widget

from murfey.client.tui.progress import BlockBarColumn


class StatusBar(Widget):
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

        task1 = progress.add_task("[red]Downloading...", total=1000)
        task2 = progress.add_task("[green]Processing...", total=1000)
        task3 = progress.add_task("[cyan]Cooking...", total=1000)
        return (progress, task1, task2, task3)

    def render(self) -> Panel:
        progress, task1, task2, task3 = self.get_progress()
        elapsed = (time.time() - self.start) * 100

        progress.update(task1, completed=max(0, min(1000, elapsed)))
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


class Hover(Widget):
    mouse_over = Reactive(False)

    def render(self) -> Panel:
        return Panel(
            "Hello [b]World[/b]",
            style=("on red" if self.mouse_over else ""),
            box=SQUARE,
        )

    def on_enter(self) -> None:
        self.mouse_over = True

    def on_leave(self) -> None:
        self.mouse_over = False


class InputBox(Widget):
    input_text: Union[Reactive[str], str] = Reactive("")
    mouse_over = Reactive(False)
    can_focus = True

    def __init__(self, app):
        self._app_reference = app
        super().__init__()

    def render(self) -> Panel:
        return Panel(
            f"[blue]❯[/blue] {self.input_text}",
            style=("on red" if self.mouse_over else ""),
            box=SQUARE,
        )

    def set_input_text(self, input_text: str) -> None:
        self.input_text = input_text

    async def on_enter(self) -> None:
        self.mouse_over = True
        await self.focus()

    async def on_leave(self) -> None:
        self.mouse_over = False
        await self._app_reference.set_focus(None)

    async def on_key(self, key: events.Key) -> None:
        if key.key == Keys.ControlH:
            self.input_text = self.input_text[:-1]
            key.stop()
        elif key.key == Keys.Delete:
            self.input_text = ""
            key.stop()
        elif key.key in string.printable:
            self.input_text += key.key
            key.stop()
        elif key.key == Keys.Enter:
            self.input_text = ""
            key.stop()


class MurfeyTUI(App):
    input_box: InputBox

    async def on_load(self, event):
        await self.bind("q", "quit")

    async def on_mount(self) -> None:
        self.input_box = InputBox(self)
        self._statusbar = StatusBar()
        hovers = (Hover() for _ in range(3))
        await self.view.dock(self._statusbar, self.input_box, *hovers, edge="top")
