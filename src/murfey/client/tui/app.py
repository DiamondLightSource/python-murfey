from __future__ import annotations

# import asyncio
# import contextlib
import logging
import string
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from queue import Queue
from typing import Callable, Dict, List, NamedTuple, TypeVar, Union
from urllib.parse import urlparse

import requests
from pydantic import BaseModel, ValidationError
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

from murfey.client.analyser import Analyser
from murfey.client.context import SPAContext, TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncer, RSyncerUpdate, TransferResult
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
    key_change_callback: Callable | None = None
    kwargs: dict | None = None
    form: dict | None = None
    model: BaseModel | None = None


class InfoWidget(Widget):
    text: Reactive[str] = Reactive("")

    def __init__(self, text: str, **kwargs):
        super().__init__(**kwargs)
        self.text = text

    def render(self) -> Panel:
        return Panel(self.text, style=("on dark_magenta"), box=SQUARE)

    def _key_change(self, input_char: str | None):
        if input_char is None:
            self.text = self.text[:-1]
            return
        self.text += input_char


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
                style=("on purple4" if self.mouse_over else "on medium_purple3"),
                box=SQUARE,
            )
        return Panel(
            self._text,
            style=("on purple4" if self.lock else "on bright_black"),
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
                        h.refresh()
                self.app.input_box.lock = False
                self.app._visit = self._text
                self.app._environment.visit = self._text
                machine_data = requests.get(
                    f"{self.app._environment.url.geturl()}/machine"
                ).json()
                if self.app._default_destination:
                    if (
                        machine_data.get("data_directory")
                        and self.app._environment.source
                        and self.app._environment.source.resolve()
                        == Path(machine_data["data_directory"])
                    ):
                        _default = self.app._default_destination + f"/{self._text}"
                    elif self.app._environment.source:
                        try:
                            mid_path = (
                                self.app._environment.source.resolve().relative_to(
                                    machine_data["data_directory"]
                                )
                            )
                            _default = f"{self.app._default_destination}/{self._text}/{mid_path}"
                        except (ValueError, KeyError):
                            _default = f"{self.app._default_destination}/{self._text}"
                    else:
                        _default = ""
                else:
                    _default = "unknown"
                self.app._queues["input"].put_nowait(
                    InputResponse(
                        question="Transfer to: ",
                        default=_default,
                        callback=self.app._start_rsyncer,
                    )
                )


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


def validate_form(form: dict, model: BaseModel) -> dict:
    log.debug("validating", form)
    try:
        validated = model(**form)
        return validated.dict()
    except (AttributeError, ValidationError) as e:
        log.debug(e)
        return {}


class InputBox(Widget):
    input_text: Union[Reactive[str], str] = Reactive("")
    prompt: str | QuickPrompt | None = ""
    mouse_over = Reactive(False)
    can_focus = True
    lock: bool = True
    current_callback: Callable | None = None
    key_change_callback: Callable | None = None
    _question: str = ""
    _form: Reactive[dict] = Reactive({})

    def __init__(self, app, queue: Queue | None = None):
        self._app_reference = app
        self._queue: Queue = queue or Queue()
        # self._form: dict = {}
        self._line = 0
        self._form_keys: List[str] = []
        self._unanswered_message = False
        self._model: BaseModel | None = None
        super().__init__()

    @property
    def _num_lines(self):
        return len(self._form.keys())

    def render(self) -> Panel:
        if not self._queue.empty() and not self.prompt and not self.input_text:
            msg = self._queue.get_nowait()
            if msg is not None:
                self._unanswered_message = True
            self.input_text = ""
            if msg.form:
                self._form = msg.form
                self._model = msg.model
            if msg.allowed_responses:
                self.prompt = QuickPrompt(msg.question, msg.allowed_responses)
                if msg.callback:
                    self.current_callback = msg.callback
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
            self._form_keys = list(self._form.keys())
            panel_msg = f"{self.input_text}\n" + "\n".join(
                f"[cyan]{key}[/cyan]: {self._form[key]}[blink]\u275a[/blink]"
                if i == self._line - 1
                else f"[cyan]{key}[/cyan]: {self._form[key]}"
                for i, key in enumerate(self._form_keys)
            )
        else:
            panel_msg = f"[white]❯[/white] {self.input_text}"
        return Panel(
            panel_msg,
            style=(
                "on blue"
                if self.mouse_over and not self._unanswered_message
                else "on deep_pink4"
                if self.mouse_over and self._unanswered_message
                else "on red"
                if self._unanswered_message
                else ""
            ),
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
        if key.key == Keys.ControlH and (
            self.input_text != self._question or self._line
        ):
            if self._line == 0:
                self.input_text = self.input_text[:-1]
                if self.key_change_callback:
                    self.key_change_callback(None)
            else:
                k = self._form_keys[self._line - 1]
                # set self._form rather than accessing by key in order to make use of reactivity
                self._form = {
                    _k: str(self._form[_k])[:-1] if _k == k else self._form[_k]
                    for _k in self._form_keys
                }
            key.stop()
        elif key.key == Keys.Delete:
            self._form = {}
            self.input_text = ""
            key.stop()
        elif key.key == Keys.Down:
            new_line = self._line + 1
            if new_line <= self._num_lines:
                self._line = new_line
            key.stop()
        elif key.key == Keys.Up:
            new_line = self._line - 1
            if new_line >= 0:
                self._line = new_line
            key.stop()
        elif key.key in string.printable:
            if self._line == 0:
                self.input_text += key.key
                if self.key_change_callback:
                    self.key_change_callback(key.key)
            else:
                k = self._form_keys[self._line - 1]
                # set self._form rather than accessing by key in order to make use of reactivity
                self._form = {
                    _k: str(self._form[_k]) + key.key if _k == k else self._form[_k]
                    for _k in self._form_keys
                }
            key.stop()
        elif key.key == Keys.Enter and self.prompt:
            if self.input_text not in self.prompt and isinstance(
                self.prompt, QuickPrompt
            ):
                self.prompt.warn = True
            else:
                self.prompt = None
                if self.current_callback:
                    self.current_callback(
                        self.input_text.replace(self._question, "", 1)
                    )
                    self.current_callback = None
                self.input_text = ""
                self._unanswered_message = False
            key.stop()
        elif key.key == Keys.Enter and self.current_callback:
            if self._form:
                if validated_form := validate_form(self._form, self._model):
                    self.current_callback(validated_form)
                    self._form = {}
                    self._form_keys = []
                else:
                    return
            else:
                self.current_callback(self.input_text.replace(self._question, "", 1))
            self.current_callback = None
            self.input_text = ""
            self._unanswered_message = False
            key.stop()
        elif key.key == Keys.Enter:
            self.input_text = ""
            self._unanswered_message = False
            if self._form:
                self._form = {}
                self._form_keys = []
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
            self.page_down()


class DCParametersTomo(BaseModel):
    voltage: float
    pixel_size_on_image: str
    experiment_type: str
    image_size_x: int
    image_size_y: int
    tilt: int
    acquisition_software: str
    dose_per_frame: float


class MurfeyTUI(App):
    input_box: InputBox
    log_book: ScrollView
    hover: List[str]
    visits: List[str]
    rsync_process: RSyncer | None = None
    analyser: Analyser | None = None

    def __init__(
        self,
        environment: MurfeyInstanceEnvironment | None = None,
        visits: List[str] | None = None,
        queues: Dict[str, Queue] | None = None,
        status_bar: StatusBar | None = None,
        dummy_dc: bool = True,
        do_transfer: bool = True,
        rsync_process: RSyncer | None = None,
        analyser: Analyser | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._environment = environment or MurfeyInstanceEnvironment(
            urlparse("http://localhost:8000")
        )
        self._source = self._environment.source or Path(".")
        self._url = self._environment.url
        self._default_destination = self._environment.default_destination
        self._watcher = self._environment.watcher
        self.visits = visits or []
        self._queues = queues or {}
        self._statusbar = status_bar or StatusBar()
        self._request_destinations = False
        self._register_dc: bool | None = None
        self._tmp_responses: List[dict] = []
        self._visit = ""
        self._dc_metadata: dict = {}
        self._dummy_dc = dummy_dc
        self._do_transfer = do_transfer
        self.rsync_process = rsync_process
        self.analyser = analyser
        self._info_widget = InfoWidget("Welcome to Murfey :microscope:")

    @property
    def role(self) -> str:
        if self.analyser:
            return self.analyser._role
        return ""

    def _start_rsyncer(self, destination: str):
        new_rsyncer = False
        if not self.rsync_process:
            self.rsync_process = RSyncer(
                self._source,
                basepath_remote=Path(destination),
                server_url=self._url,
                local=self._environment.demo,
                status_bar=self._statusbar,
                do_transfer=self._do_transfer,
            )
            new_rsyncer = True
        else:
            if self._environment.demo:
                _remote = destination
            else:
                _remote = f"{self._url.hostname}::{destination}"
            self.rsync_process._remote = _remote
            self.thread = threading.Thread(
                name=f"RSync {self._source.absolute()}:{_remote}",
                target=self.rsync_process._process,
            )

        def rsync_result(update: RSyncerUpdate):
            if not self.rsync_process:
                raise ValueError("TUI rsync process does not exist")
            if update.outcome is TransferResult.SUCCESS:
                log.info(
                    f"File {str(update.file_path)!r} successfully transferred ({update.file_size} bytes)"
                )
            else:
                log.warning(f"Failed to transfer file {str(update.file_path)!r}")
                self.rsync_process.enqueue(update.file_path)

        if self.rsync_process:
            self.rsync_process.subscribe(rsync_result)
            self.rsync_process.start()
            new_analyser = False
            if not self.analyser:
                self.analyser = Analyser(
                    environment=self._environment if not self._dummy_dc else None
                )
                new_analyser = True
            if self._watcher:
                if new_rsyncer:
                    self._watcher.subscribe(self.rsync_process.enqueue)
                if new_analyser:
                    self._watcher.subscribe(self.analyser.enqueue)
            self.analyser.subscribe(self._data_collection_form)
            self.analyser.start()

    def _set_register_dc(self, response: str):
        if response == "y":
            self._register_dc = True
            for r in self._tmp_responses:
                self._queues["input"].put_nowait(
                    InputResponse(
                        question="Data collection parameters:",
                        form=r.get("form", {}),
                        model=DCParametersTomo
                        if self.analyser
                        and isinstance(self.analyser._context, TomographyContext)
                        else None,
                        callback=self.app._start_dc,
                    )
                )
                self._dc_metadata = r.get("form", {})
        elif response == "n":
            self._register_dc = False
        self._tmp_responses = []

    def _data_collection_form(self, response: dict):
        if self._register_dc and response.get("form"):
            self._queues["input"].put_nowait(
                InputResponse(
                    question="Data collection parameters:", form=response["form"]
                )
            )
        elif response.get("allowed_responses"):
            self._queues["input"].put_nowait(
                InputResponse(
                    question="Would you like to start a data collection?",
                    allowed_responses=response["allowed_responses"],
                    callback=self._set_register_dc,
                )
            )
        elif self._register_dc is None:
            self._tmp_responses.append(response)

    def _start_dc(self, json):
        if self._dummy_dc:
            return
        self._environment.data_collection_parameters = json
        if isinstance(self.analyser._context, TomographyContext):
            self._environment.listeners["data_collection_group_id"] = {
                self.analyser._context._flush_data_collections
            }
            self._environment.listeners["data_collection_ids"] = {
                self.analyser._context._flush_processing_job
            }
            self._environment.listeners["autoproc_program_ids"] = {
                self.analyser._context._flush_preprocess
            }
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/register_data_collection_group"
            dcg_data = {"experiment_type": "tomo"}
            requests.post(url, json=dcg_data)
        elif isinstance(self.analyser._context, SPAContext):
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/register_data_collection_group"
            dcg_data = {"experiment_type": "single particle"}
            requests.post(url, json=dcg_data)
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/start_data_collection"
            requests.post(url, json=json)

    def _update_info(self, new_text: str):
        self._info_widget.text = new_text

    def _set_request_destination(self, response: str):
        if response == "y":
            self._request_destinations = True

    async def on_load(self, event):
        await self.bind("q", "quit", show=True)

    async def on_mount(self) -> None:
        self.input_box = InputBox(self, queue=self._queues.get("input"))
        self._queues["input"].put_nowait(
            InputResponse(
                question="Are you using multi-grid?",
                allowed_responses=["y", "n"],
                callback=self._set_request_destination,
            )
        )
        # self._queues["input"].put_nowait(
        #     InputResponse(
        #         question="Processing parameters: ",
        #         form={"Voltage [keV]": 300, "Pixel size [U+212b]": 1},
        #     )
        # )
        self.log_book = LogBook(self._queues["logs"])
        # self._statusbar = StatusBar()
        self.hovers = (
            [HoverVisit(v) for v in self.visits]
            if len(self.visits)
            else [HoverVisit("No ongoing visits found")]
        )

        grid = await self.view.dock_grid(edge="left")

        grid.add_column(fraction=1, name="left")
        grid.add_column(fraction=1, name="right")
        grid.add_row(fraction=1, name="top")
        grid.add_row(fraction=1, name="middle")
        grid.add_row(fraction=1, name="bottom")

        grid.add_areas(
            area1="left,top",
            area2="right,top-start|bottom-end",
            # area3="left,middle",
            area3="left,middle-start|bottom-end",
        )

        sub_view = DockView()
        await sub_view.dock(*self.hovers, edge="top")

        info_sub_view = DockView()
        await info_sub_view.dock(self.input_box, self._info_widget, edge="top")

        grid.place(
            area1=sub_view,
            area2=self.log_book,
            # area3=self._statusbar,
            # area4=self.input_box,
            area3=info_sub_view,
        )

    async def action_quit(self) -> None:
        if self.rsync_process:
            self.rsync_process.stop()
        if self.analyser:
            self.analyser.stop()
        await self.shutdown()
