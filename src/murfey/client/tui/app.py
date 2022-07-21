from __future__ import annotations

# import asyncio
# import contextlib
import logging
import string
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from typing import Callable, Dict, List, NamedTuple, TypeVar, Union
from urllib.parse import urlparse

from rich.box import SQUARE
from rich.logging import RichHandler
from rich.panel import Panel
from textual import events
from textual.app import App
from textual.keys import Keys
from textual.reactive import Reactive
from textual.views import DockView
from textual.widget import Widget
from textual.widgets import ScrollView

import murfey.client.rsync as rsync
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncer
from murfey.client.tui.status_bar import StatusBar

log = logging.getLogger("murfey.tui.app")

ReactiveType = TypeVar("ReactiveType")

_pool = ThreadPoolExecutor()

# @contextlib.asynccontextmanager
# async def async_lock(lock):
#     loop = asyncio.get_event_loop()
#     await loop.run_in_executor(_pool, lock.acquire)
#     try:
#         yield
#     finally:
#         lock.release()


class InputResponse(NamedTuple):
    question: str
    allowed_responses: List[str] | None = None
    default: str = ""
    callback: Callable | None = None
    kwargs: dict | None = None
    form: dict | None = None


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
    prompt: str | QuickPrompt | None = ""
    mouse_over = Reactive(False)
    can_focus = True
    lock: bool = True
    current_callback: Callable | None = None
    _question: str = ""

    def __init__(self, app, queue: Queue | None = None):
        self._app_reference = app
        self._queue: Queue = queue or Queue()
        self._form: dict = {}
        super().__init__()

    def render(self) -> Panel:
        if not self._queue.empty() and not self.prompt and not self.input_text:
            msg = self._queue.get_nowait()
            self.input_text = ""
            if msg.form:
                self._form = msg.form
            if msg.allowed_responses:
                self.prompt = QuickPrompt(msg.question, msg.allowed_responses)
            else:
                self._question = msg.question
                self.input_text = msg.question + msg.default
                if msg.callback:
                    self.current_callback = msg.callback
        if isinstance(self.prompt, QuickPrompt):
            panel_msg = (
                f"{self.prompt}: [[red]{'/'.join(self.prompt)}[/red]] {self.input_text}"
                if self.prompt.warn
                else f"{self.prompt}: [[white]{'/'.join(self.prompt)}[/white]] {self.input_text}"
            )
        elif self._form:
            panel_msg = f"{self.input_text}\n" + "\n".join(
                f"{key}: {value}" for key, value in self._form.items()
            )
        else:
            panel_msg = f"[white]❯[/white] {self.input_text}"
        return Panel(
            panel_msg,
            style=("on blue" if self.mouse_over else ""),
            box=SQUARE,
        )

    def on_mount(self):
        self.set_interval(2, self.tick)

    def tick(self):
        self.refresh()

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
        if key.key == Keys.ControlH and self.input_text != self._question:
            self.input_text = self.input_text[:-1]
            key.stop()
        elif key.key == Keys.Delete:
            self.input_text = ""
            key.stop()
        elif key.key in string.printable:
            self.input_text += key.key
            key.stop()
        elif key.key == Keys.Enter and self.prompt:
            if self.input_text not in self.prompt and isinstance(
                self.prompt, QuickPrompt
            ):
                self.prompt.warn = True
            else:
                self.prompt = None
                self.input_text = ""
            key.stop()
        elif key.key == Keys.Enter and self.current_callback:
            self.current_callback(self.input_text.replace(self._question, "", 1))
            self.input_text = ""
            key.stop()
        elif key.key == Keys.Enter:
            self.input_text = ""
            key.stop()


class LogBook(ScrollView):
    def __init__(self, queue, *args, **kwargs):
        self._queue = queue
        self._next_log = None
        self._logs = None
        self._handler = RichHandler(enable_link_path=False)
        super().__init__(*args, **kwargs)

    def on_mount(self):
        self.set_interval(0.5, self.tick)

    def _load_from_queue(self) -> bool:
        if not self._queue.empty():
            num_logs = 0
            self._next_log = []
            while not self._queue.empty() and num_logs < 10:
                msg = self._queue.get_nowait()
                self._next_log.append(msg)
                num_logs += 1
            return True
        return False

    async def tick(self):
        loaded = self._load_from_queue()
        if loaded:
            if self._logs is None:
                self._logs = self._next_log[0][1]
                for nl in self._next_log[1:]:
                    self._logs.add_row(*nl[0])
            else:
                for nl in self._next_log:
                    self._logs.add_row(*nl[0])
            await self.update(self._logs, home=False)


class MurfeyTUI(App):
    input_box: InputBox
    log_book: ScrollView
    hover: List[str]
    visits: List[str]
    rsync_process: RSyncer | None = None

    def __init__(
        self,
        environment: MurfeyInstanceEnvironment | None = None,
        visits: List[str] | None = None,
        queues: Dict[str, Queue] | None = None,
        status_bar: StatusBar | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._environment = environment or MurfeyInstanceEnvironment(
            urlparse("http://localhost:8000")
        )
        self._source = self._environment.source or Path(".")
        self._url = self._environment.murfey_url
        self._default_destination = self._environment.default_destination
        self._watcher = self._environment.watcher
        self.visits = visits or []
        self._queues = queues or {}
        self._statusbar = status_bar or StatusBar()

    def _start_rsyncer(self, destination: str):
        self.rsync_process = rsync.RSyncer(
            self._source,
            basepath_remote=Path(destination),
            server_url=self._url,
            local=self._environment.demo,
        )

        def rsync_result(update: rsync.RSyncerUpdate):
            if not self.rsync_process:
                raise ValueError("TUI rsync process does not exist")
            if update.outcome is rsync.TransferResult.SUCCESS:
                log.info(
                    f"File {str(update.file_path)!r} successfully transferred ({update.file_size} bytes)"
                )
            else:
                log.warning(f"Failed to transfer file {str(update.file_path)!r}")
                self.rsync_process.enqueue(update.file_path)

        if self.rsync_process:
            self.rsync_process.subscribe(rsync_result)
            self.rsync_process.start()
            if self._watcher:
                self._watcher.subscribe(self.rsync_process.enqueue)

    async def on_load(self, event):
        await self.bind("q", "quit", show=True)

    async def on_mount(self) -> None:
        self.input_box = InputBox(self, queue=self._queues.get("input"))
        self._queues["input"].put_nowait(
            InputResponse(
                question="Transfer to: ",
                default=self._default_destination or "unknown",
                callback=self._start_rsyncer,
            )
        )
        self._queues["input"].put_nowait(
            InputResponse(
                question="Would you like to register a new data collection?",
                allowed_responses=["y", "n"],
            )
        )
        self._queues["input"].put_nowait(
            InputResponse(
                question="Processing parameters: ",
                form={"Voltage [keV]": 300, "Pixel size [U+212b]": 1},
            )
        )
        self.log_book = LogBook(self._queues["logs"])
        # self._statusbar = StatusBar()
        self.hovers = (
            [HoverVisit(v) for v in self.visits]
            if len(self.visits)
            else [Hover("No ongoing visits found")]
        )

        grid = await self.view.dock_grid(edge="left")

        grid.add_column(fraction=2, name="left")
        grid.add_column(fraction=1, name="right")
        grid.add_row(fraction=1, name="top")
        grid.add_row(fraction=1, name="middle")
        grid.add_row(fraction=1, name="bottom")

        grid.add_areas(
            area1="left,top",
            area2="right,top-start|bottom-end",
            area3="left,middle",
            area4="left,bottom",
        )

        sub_view = DockView()
        await sub_view.dock(*self.hovers, edge="top")

        grid.place(
            area1=sub_view,
            area2=self.log_book,
            area3=self._statusbar,
            area4=self.input_box,
        )

    async def action_quit(self) -> None:
        if self.rsync_process:
            self.rsync_process.stop()
        await self.shutdown()
