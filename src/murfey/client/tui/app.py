from __future__ import annotations

import asyncio

# import contextlib
import logging
import threading
from functools import partial
from pathlib import Path
from queue import Queue
from typing import Callable, Dict, List, NamedTuple, Optional, OrderedDict, TypeVar
from urllib.parse import urlparse

import procrunner
import requests
from pydantic import BaseModel, ValidationError
from rich.box import SQUARE
from rich.panel import Panel
from textual.app import App, ComposeResult, ScreenStackError
from textual.containers import Vertical
from textual.reactive import Reactive
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import (
    Button,
    DataTable,
    DirectoryTree,
    Footer,
    Header,
    Input,
    Label,
    Static,
    TextLog,
    Tree,
)

from murfey.client.analyser import Analyser
from murfey.client.context import SPAContext, TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncer, RSyncerUpdate, TransferResult
from murfey.client.tui.forms import TUIFormValue
from murfey.client.tui.status_bar import StatusBar

log = logging.getLogger("murfey.tui.app")

ReactiveType = TypeVar("ReactiveType")


class InputResponse(NamedTuple):
    question: str
    allowed_responses: List[str] | None = None
    default: str = ""
    callback: Callable | None = None
    key_change_callback: Callable | None = None
    kwargs: dict | None = None
    form: OrderedDict[str, TUIFormValue] | None = None
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
    try:
        validated = model(**form)
        log.info(validated.dict())
        return validated.dict()
    except (AttributeError, ValidationError) as e:
        log.warning(f"Form validation failed: {str(e)}")
        return {}


class DCParametersTomo(BaseModel):
    dose_per_frame: float
    gain_ref: Optional[str]
    experiment_type: str
    voltage: float
    image_size_x: int
    image_size_y: int
    pixel_size_on_image: str
    motion_corr_binning: int
    tilt_offset: float
    file_extension: str
    acquisition_software: str


class _DirectoryTree(DirectoryTree):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._selected_path = self.path

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        event.stop()
        dir_entry = event.node.data
        if dir_entry is None:
            return
        if dir_entry.is_dir:
            self._selected_path = dir_entry.path
        else:
            self.emit_no_wait(self.FileSelected(self, dir_entry.path))


class LaunchScreen(Screen):
    _selected_dir = Path(".")

    def compose(self):
        self._dir_tree = _DirectoryTree("./", id="dir-select")
        yield self._dir_tree
        yield Button("Launch", id="launch")
        yield Button("Quit", id="quit")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "quit":
            self.app.exit()
            exit()
        else:
            self.app._environment.source = (
                Path(self._dir_tree.path).resolve() / self._dir_tree._selected_path
            )
            self.app._info_widget.write(
                f"{Path(self._dir_tree.path).resolve() / self._dir_tree._selected_path}"
            )
            self.app.pop_screen()


class ConfirmScreen(Screen):
    def __init__(
        self,
        prompt: str,
        *args,
        params: dict | None = None,
        pressed_callback: Callable | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._prompt = prompt
        self._params = params or {}
        self._callback = pressed_callback

    def compose(self):
        if self._params:
            dt = DataTable(id="prompt")
            keys = list(self._params.keys())
            dt.add_columns(*keys)
            dt.add_rows([[self._params[k] for k in keys]])
            yield dt
        else:
            yield Static(self._prompt, id="prompt")
        yield Button("Launch", id="launch")
        yield Button("Back", id="quit")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "quit":
            self.app.pop_screen()
        else:
            while True:
                try:
                    self.app.pop_screen()
                except ScreenStackError:
                    break
            self.app.uninstall_screen("confirm")
        if self._callback and event.button.id == "launch":
            self._callback(params=self._params)


class ProcessingForm(Screen):
    _form = Reactive({})
    _vert = None

    def __init__(self, form: dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._form = form
        self._readable_labels = {
            "experiment_type": "Experiment Type",
            "voltage": "Voltage",
            "image_size_x": "Image Size X",
            "image_size_y": "Image Size Y",
            "pixel_size_on_image": "Pixel Size",
            "motion_corr_binning": "Motion Correction Binning",
            "gain_ref": "Gain Reference",
            "dose_per_frame": "Dose Per Frame",
            "tilt_offset": "Tilt Offset",
            "file_extension": "File Extension",
            "acquisition_software": "Acquisition Software",
        }
        self._inputs: Dict[Input, str] = {}

    def compose(self):
        inputs = []
        for k, v in self._form.items():
            t = self._readable_labels.get(k, k)
            inputs.append(Label(t, classes="label"))
            i = Input(placeholder=t, classes="input")
            self._inputs[i] = k
            i.value = v
            inputs.append(i)
        confirm_btn = Button("Confirm", id="confirm-btn")
        self._vert = Vertical(*inputs, confirm_btn, id="input-form")
        yield self._vert
        yield confirm_btn

    def _write_params(self, params: dict | None = None):
        if params:
            for k, v in params.items():
                self.app._info_widget.write(f"{self._readable_labels.get(k, k)}: {v}")

    def on_input_submitted(self, event):
        k = self._inputs[event.input]
        self._form[k] = event.value

    def on_button_pressed(self, event):
        # for k, v in self._form.items():
        #    self.app._info_widget.write(f"{self._readable_labels.get(k, k)}: {v}")
        if "confirm" not in self.app._installed_screens:
            self.app.install_screen(
                ConfirmScreen(
                    "Launch processing?",
                    params={
                        self._readable_labels.get(k, k): v
                        for k, v in self._form.items()
                    },
                    pressed_callback=self._write_params,
                ),
                "confirm",
            )
        self.app.push_screen("confirm")


class VisitSelection(Screen):
    def __init__(self, visits: List[str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._visits = visits

    def compose(self):
        hovers = (
            [Button(v, id="visit-btn") for v in self._visits]
            if self._visits
            else [Button("No ongoing visits found")]
        )
        yield Vertical(*hovers, id="visit-select")

    def on_button_pressed(self, event: Button.Pressed):
        text = str(event.button.label)
        self.app._visit = text
        self.app._environment.visit = text
        machine_data = requests.get(
            f"{self.app._environment.url.geturl()}/machine/"
        ).json()
        _default = ""
        visit_path = ""
        if self.app._default_destination:
            visit_path = self.app._default_destination + f"/{text}"
            if (
                self.app._environment.processing_only_mode
                and self.app._environment.source
            ):
                _default = str(self.app._environment.source.resolve()) or str(
                    Path.cwd()
                )
            elif machine_data.get("data_directories"):
                for data_dir in machine_data["data_directories"].keys():
                    if (
                        self.app._environment.source
                        and self.app._environment.source.resolve() == Path(data_dir)
                    ):
                        _default = self.app._default_destination + f"/{text}"
                        if self.app.analyser:
                            self.app.analyser._role = machine_data["data_directories"][
                                data_dir
                            ]
                        break
                    elif self.app._environment.source:
                        try:
                            mid_path = (
                                self.app._environment.source.resolve().relative_to(
                                    data_dir
                                )
                            )
                            if machine_data["data_directories"][data_dir] == "detector":
                                suggested_path_response = requests.post(
                                    url=f"{str(self.app._url.geturl())}/visits/{text}/suggested_path",
                                    json={
                                        "base_path": f"{self.app._default_destination}/{text}/{mid_path.parent}/raw"
                                    },
                                )
                                _default = suggested_path_response.json().get(
                                    "suggested_path"
                                )
                            else:
                                _default = (
                                    f"{self.app._default_destination}/{text}/{mid_path}"
                                )
                            if self.app.analyser:
                                self.app.analyser._role = machine_data[
                                    "data_directories"
                                ][data_dir]
                            break
                        except (ValueError, KeyError):
                            _default = ""
                else:
                    _default = ""
            else:
                _default = self.app._default_destination + f"/{text}"
        else:
            _default = "unknown"
        if self.app._environment.processing_only_mode:
            self.app._start_rsyncer(_default, visit_path=visit_path)
        else:
            self.app._queues["input"].put_nowait(
                InputResponse(
                    question="Transfer to: ",
                    default=_default,
                    callback=partial(self.app._start_rsyncer, visit_path=visit_path),
                )
            )
        self.app.install_screen(
            DestinationSelect(_default), "destination-select-screen"
        )
        self.app.pop_screen()
        self.app.push_screen("destination-select-screen")


class DestinationSelect(Screen):
    def __init__(self, destination: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._destination = destination
        self._input = Input(placeholder="Destination")

    def compose(self):
        yield self._input

    def on_mount(self):
        self._input.value = self._destination
        self._input.focus()

    def on_input_submitted(self, event):
        self._destination = event.value
        self.app._default_destination = self._destination
        self.app._register_dc = True
        self.app._start_rsyncer(self._destination)
        self.app.pop_screen()


class MurfeyTUI(App):
    CSS_PATH = "controller.css"
    SCREENS = {"launcher": LaunchScreen()}
    log_book: TextLog
    processing_btn: Button
    processing_form: ProcessingForm
    hover: List[str]
    visits: List[str]
    rsync_process: RSyncer | None = None
    analyser: Analyser | None = None
    _form_values: dict = Reactive({})

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
        gain_ref: Path | None = None,
        redirected_logger=None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._environment = environment or MurfeyInstanceEnvironment(
            urlparse("http://localhost:8000")
        )
        self._environment.gain_ref = gain_ref
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
        self._data_collection_form_complete = False
        self._info_widget = TextLog(id="info", markup=True)
        self._form_readable_labels: dict = {}
        self._redirected_logger = redirected_logger

    @property
    def role(self) -> str:
        if self.analyser:
            return self.analyser._role
        return ""

    def _start_rsyncer(self, destination: str, visit_path: str = ""):
        new_rsyncer = False
        if self._environment:
            self._environment.default_destination = destination
            if self._environment.gain_ref and visit_path:
                gain_rsync = procrunner.run(
                    [
                        "rsync",
                        str(self._enviornment.gain_ref),
                        f"{self._url.hostname}::{visit_path}/processing",
                    ]
                )
                if gain_rsync.returncode:
                    log.warning(
                        f"Gain reference file {self._environment.gain_ref} was not successfully transferred to {visit_path}/processing"
                    )
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

        self._info_widget.write(f"{self._source.resolve()} \u2192 {destination}")

        def rsync_result(update: RSyncerUpdate):
            if not self.rsync_process:
                raise ValueError("TUI rsync process does not exist")
            if update.outcome is TransferResult.SUCCESS:
                # log.info(
                #     f"File {str(update.file_path)!r} successfully transferred ({update.file_size} bytes)"
                # )
                pass
            else:
                log.warning(f"Failed to transfer file {str(update.file_path)!r}")
                self.rsync_process.enqueue(update.file_path)

        if self.rsync_process:
            self.rsync_process.subscribe(rsync_result)
            self.rsync_process.start()
            new_analyser = False
            if not self.analyser:
                self.analyser = Analyser(
                    self._source,
                    environment=self._environment if not self._dummy_dc else None,
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
                        form=r.get("form", OrderedDict({})),
                        model=DCParametersTomo
                        if self.analyser
                        and isinstance(self.analyser._context, TomographyContext)
                        else None,
                        callback=self.app._start_dc_confirm_prompt,
                    )
                )
                self._dc_metadata = r.get("form", OrderedDict({}))
        elif response == "n":
            self._register_dc = False
        self._tmp_responses = []

    def _data_collection_form(self, response: dict):
        if self._data_collection_form_complete:
            return
        if self._register_dc and response.get("form"):
            self._form_values = {k: str(v) for k, v in response.get("form", {}).items()}
            self.processing_btn.disabled = False
            self._data_collection_form_complete = True
        elif self._register_dc is None:
            self._tmp_responses.append(response)
            self._data_collection_form_complete = True

    def _start_dc_confirm_prompt(self, json: dict):
        self._queues["input"].put_nowait(
            InputResponse(
                question="Would you like to start processing with chosen parameters?",
                allowed_responses=["y", "n"],
                callback=partial(self._start_dc_confirm, json=json),
            )
        )

    def _start_dc_confirm(self, response: str, json: Optional[dict] = None):
        json = json or {}
        if response == "n":
            self._queues["input"].put_nowait(
                InputResponse(
                    question="Data collection parameters:",
                    form=OrderedDict({k: TUIFormValue(v) for k, v in json.items()}),
                    model=DCParametersTomo
                    if self.analyser
                    and isinstance(self.analyser._context, TomographyContext)
                    else None,
                    callback=self.app._start_dc_confirm_prompt,
                )
            )
        elif response == "y":
            self._start_dc(json)

    def _start_dc(self, json):
        if self._dummy_dc:
            return
        self._environment.data_collection_parameters = {
            k: None if v == "None" else v for k, v in json.items()
        }
        for k, v in json.items():
            self._info_widget.write(f"{k}: {v}")
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
            self._environment.listeners["motion_corrected_movies"] = {
                self.analyser._context._check_for_alignment
            }
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/register_data_collection_group"
            dcg_data = {"experiment_type": "tomo", "experiment_type_id": 36}
            requests.post(url, json=dcg_data)
        elif isinstance(self.analyser._context, SPAContext):
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/register_data_collection_group"
            dcg_data = {"experiment_type": "single particle", "experiment_type_id": 37}
            requests.post(url, json=dcg_data)
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/start_data_collection"
            requests.post(url, json=json)

    def _update_info(self, new_text: str):
        self._info_widget.write(new_text)

    def _set_request_destination(self, response: str):
        if response == "y":
            self._request_destinations = True

    async def on_load(self, event):
        self.bind("q", "quit", description="Quit", show=True)
        self.bind("c", "clear", description="Remove copied data and quit", show=True)
        self.bind("p", "process", description="Allow processing", show=True)

    def _install_processing_form(self):
        self.processing_form = ProcessingForm(self._form_values)
        self.install_screen(self.processing_form, "processing-form")

    def compose(self) -> ComposeResult:
        self.log_book = TextLog(id="log_book", wrap=True, max_lines=200)
        if self._redirected_logger:
            log.info("connecting logger")
            self._redirected_logger.text_log = self.log_book
            log.info("logger connected")
        self.hovers = (
            [Button(v, id="visit-btn") for v in self.visits]
            if len(self.visits)
            else [Button("No ongoing visits found")]
        )
        inputs = []
        for t in (
            "Pixel Size",
            "Magnification",
            "Image Size X",
            "Image Size Y",
            "Dose",
            "Gain Reference",
        ):
            inputs.append(Label(t, classes="label"))
            inputs.append(Input(placeholder=t, classes="input"))
        self.processing_form = ProcessingForm(self._form_values)
        yield Header()
        yield self._info_widget
        yield self.log_book
        self.processing_btn = Button(
            "Request processing", id="processing-btn", disabled=not self._form_values
        )
        yield self.processing_btn
        yield Footer()

    def on_input_submitted(self, event: Input.Submitted):
        event.input.has_focus = False
        self.screen.focused = None

    def on_button_pressed(self, event: Button.Pressed):
        if event.button._id == "processing-btn":
            self._install_processing_form()
            self.push_screen("processing-form")

    async def on_mount(self) -> None:
        self._info_widget.write("[bold]Welcome to Murfey[/bold]")
        self.install_screen(VisitSelection(self.visits), "visit-select-screen")
        self.push_screen("visit-select-screen")
        self.push_screen("launcher")

    async def action_quit(self) -> None:
        if self.rsync_process:
            self.rsync_process.stop()
        if self.analyser:
            self.analyser.stop()
        self.exit()
        exit()

    async def action_clear(self) -> None:
        destination = ""
        if self.rsync_process:
            destination = (
                self.rsync_process._remote.split("::")[1]
                if "::" in self.rsync_process._remote
                else self.rsync_process._remote
            )
        self._queues["input"].put_nowait(
            InputResponse(
                question=f"Are you sure you want to remove all copied data? [{self._source} -> {destination}]",
                allowed_responses=["y", "n"],
                callback=partial(self._confirm_clear),
            )
        )

    async def action_process(self) -> None:
        self.processing_btn.disabled = False

    def _confirm_clear(self, response: str):
        if response == "y":
            if self._do_transfer and self.rsync_process:
                destination = self.rsync_process._remote
                self.rsync_process.stop()
                if self.analyser:
                    self.analyser.stop()
                cmd = [
                    "rsync",
                    "-iiv",
                    "-o",  # preserve ownership
                    "-p",  # preserve permissions
                    "--remove-source-files",
                ]
                cmd.extend(
                    str(f.relative_to(self._source.absolute()))
                    for f in self._source.absolute().glob("**/*")
                )
                cmd.append(destination)
                result = procrunner.run(cmd)
                log.info(
                    f"rsync with removal finished with return code {result.returncode}"
                )

            loop = asyncio.get_running_loop()
            loop.create_task(self.action_quit())
