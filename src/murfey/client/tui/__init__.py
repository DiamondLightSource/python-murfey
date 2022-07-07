from __future__ import annotations

import functools
import string
import time
from datetime import datetime
from queue import Queue
from typing import List, Union

from rich.align import Align
from rich.box import SQUARE
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import (
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.prompt import Prompt
from rich.table import Column
from textual import events
from textual.app import App
from textual.keys import Keys
from textual.reactive import Reactive
from textual.widget import Widget
from textual.widgets import ScrollView

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

    def __init__(self, text: str, **kwargs):
        super().__init__(**kwargs)
        self._text = text

    def render(self) -> Panel:
        return Panel(
            self._text,
            style=("on red" if self.mouse_over else ""),
            box=SQUARE,
        )

    def on_enter(self) -> None:
        self.mouse_over = True

    def on_leave(self) -> None:
        self.mouse_over = False

    async def on_click(self) -> None:
        await self.app.shutdown()


class HoverVisit(Widget):
    mouse_over = Reactive(False)
    lock: bool | None = None

    def __init__(self, text: str, **kwargs):
        super().__init__(**kwargs)
        self._text = text

    def render(self) -> Panel:
        if self.lock is None:
            return Panel(
                self._text,
                style=("on red" if self.mouse_over else ""),
                box=SQUARE,
            )
        return Panel(
            self._text,
            style=("on red" if self.lock else ""),
            box=SQUARE,
        )

    def on_enter(self) -> None:
        self.mouse_over = True

    def on_leave(self) -> None:
        self.mouse_over = False

    def on_click(self) -> None:
        if self.lock is None:
            self.lock = True
            if isinstance(self.app, MurfeyTUI):
                for h in self.app.hovers:
                    if isinstance(h, HoverVisit) and h != self:
                        h.lock = False
                self.app.input_box.lock = False


class QuickPrompt:
    def __init__(self, text: str, options: List[str]):
        self._text = text
        self._options = options
        self.warn = False

    def __repr__(self):
        return repr(self._text)

    def __str__(self):
        return self._text

    def __iter__(self):
        return iter(self._options)

    def __bool__(self):
        return bool(self._text)


class InputBox(Widget):
    input_text: Union[Reactive[str], str] = Reactive("")
    prompt: str = ""
    mouse_over = Reactive(False)
    can_focus = True
    lock: bool = True

    def __init__(self, app, queue: Queue | None = None):
        self._app_reference = app
        self._queue: Queue = queue or Queue()
        super().__init__()

    def render(self) -> Panel:
        if not self._queue.empty():
            msg = self._queue.get_nowait()
            self.input_text = ""
            self.prompt = QuickPrompt(msg[0], msg[1])
        if self.prompt:
            panel_msg = (
                f"{self.prompt}: [[red]{'/'.join(self.prompt)}[/red]] {self.input_text}"
                if self.prompt.warn
                else f"{self.prompt}: [[white]{'/'.join(self.prompt)}[/white]] {self.input_text}"
            )
        else:
            panel_msg = f"[white]❯[/white] {self.input_text}"
        return Panel(
            panel_msg,
            style=("on blue" if self.mouse_over else ""),
            box=SQUARE,
        )

    def set_input_text(self, input_text: str) -> None:
        self.input_text = input_text

    async def on_enter(self) -> None:
        if not self.lock:
            self.mouse_over = True
            await self.focus()

    async def on_leave(self) -> None:
        if not self.lock:
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
        elif key.key == Keys.Enter and self.prompt:
            if self.input_text not in self.prompt:
                self.prompt.warn = True
            else:
                self.prompt = None
                self.input_text = ""
            key.stop()
        elif key.key == Keys.Enter:
            self.input_text = ""
            key.stop()


# class LogBook(ScrollView):
#     def __init__(self, queue: Queue, *args, **kwargs):
#         self._queue = queue
#         self._handler = Reactive(RichHandler(enable_link_path=False))
#         super().__init__(*args, **kwargs)

#     async def on_mount(self) -> None:


class MurfeyTUI(App):
    input_box: InputBox
    log_book: ScrollView
    hover: List[str]
    visits: List[str]
    _handler = Reactive(RichHandler(enable_link_path=False))

    def __init__(
        self,
        visits: List[str] | None = None,
        queues: Dict[str, Queue] | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.visits = visits or []
        self._queues = queues

    async def on_load(self, event):
        await self.bind("q", "quit", show=True)

    async def on_mount(self) -> None:
        self.input_box = InputBox(self, queue=self._queues.get("input"))
        self.log_book = ScrollView()
        await self.log_book.update(self._handler)
        self._statusbar = StatusBar()
        self.hovers = (
            [HoverVisit(v) for v in self.visits]
            if len(self.visits)
            else [Hover("No ongoing visits found")]
        )
        await self.view.dock(*self.hovers, self._statusbar, self.input_box, edge="top")
        await self.view.dock(self.log_book, edge="right")

        # async def add_log():
        #     if self._queues.get("logs") and not self._queues["logs"].empty():
        #         record = self._queues["logs"].get_nowait()
        #         self._handler.emit(record)
        #     await self.log_book.update(self._handler.console)

        # await self.call_later(add_log)
