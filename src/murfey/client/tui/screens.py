from __future__ import annotations

# import contextlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, List, NamedTuple, OrderedDict, TypeVar

import requests
from pydantic import BaseModel, ValidationError
from rich.box import SQUARE
from rich.panel import Panel
from textual.app import ScreenStackError
from textual.containers import Vertical
from textual.reactive import reactive
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
    Switch,
    TextLog,
    Tree,
)

from murfey.client.analyser import Analyser
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.tui.forms import TUIFormValue

log = logging.getLogger("murfey.tui.screens")

ReactiveType = TypeVar("ReactiveType")


def determine_default_destination(
    visit: str,
    source: Path,
    destination: str,
    environment: MurfeyInstanceEnvironment,
    analysers: Dict[Path, Analyser],
    touch: bool = False,
):
    machine_data = requests.get(f"{environment.url.geturl()}/machine/").json()
    _default = ""
    if environment.processing_only_mode and environment.source:
        _default = str(environment.source.resolve()) or str(Path.cwd())
    elif machine_data.get("data_directories"):
        for data_dir in machine_data["data_directories"].keys():
            if source.resolve() == Path(data_dir):
                _default = destination + f"/{visit}"
                if analysers.get(source):
                    analysers[source]._role = machine_data["data_directories"][data_dir]
                break
            else:
                try:
                    mid_path = source.resolve().relative_to(data_dir)
                    if machine_data["data_directories"][data_dir] == "detector":
                        suggested_path_response = requests.post(
                            url=f"{str(environment.url.geturl())}/visits/{visit}/suggested_path",
                            json={
                                "base_path": f"{destination}/{visit}/{mid_path.parent}/raw",
                                "touch": touch,
                            },
                        )
                        _default = suggested_path_response.json().get("suggested_path")
                    else:
                        _default = f"{destination}/{visit}/{mid_path}"
                    if analysers.get(source):
                        analysers[source]._role = machine_data["data_directories"][
                            data_dir
                        ]
                    break
                except (ValueError, KeyError):
                    _default = ""
        else:
            _default = ""
    else:
        _default = destination + f"/{visit}"
    return _default


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
    text: reactive[str] = reactive("")

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


class _DirectoryTree(DirectoryTree):
    valid_selection = reactive(False)

    def __init__(self, *args, data_directories: dict | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self._selected_path = self.path
        self._data_directories = data_directories or {}

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        event.stop()
        dir_entry = event.node.data
        if dir_entry is None:
            return
        if dir_entry.is_dir:
            self._selected_path = dir_entry.path
            for d in self._data_directories:
                if Path(self._selected_path).resolve().is_relative_to(d):
                    self.valid_selection = True
                    break
            else:
                self.valid_selection = False
        else:
            self.valid_selection = False
            self.emit_no_wait(self.FileSelected(self, dir_entry.path))


class LaunchScreen(Screen):
    # _selected_dir = Path(".")
    _launch_btn: Button | None = None

    def __init__(
        self, *args, basepath: Path = Path("."), add_basepath: bool = False, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._selected_dir = basepath
        self._add_basepath = add_basepath

    def compose(self):
        machine_data = requests.get(
            f"{self.app._environment.url.geturl()}/machine/"
        ).json()
        self._dir_tree = _DirectoryTree(
            str(self._selected_dir),
            data_directories=machine_data.get("data_directories", {}),
            id="dir-select",
        )

        yield self._dir_tree
        text_log = TextLog(id="selected-directories")
        yield text_log

        text_log.write("Selected directories:\n")
        btn_disabled = True
        for d in machine_data.get("data_directories", {}).keys():
            if Path(self._dir_tree._selected_path).resolve().is_relative_to(d):
                btn_disabled = False
                break
        self._launch_btn = Button("Launch", id="launch", disabled=btn_disabled)
        self._add_btn = Button("Add directory", id="add", disabled=btn_disabled)
        self.watch(self._dir_tree, "valid_selection", self._check_valid_selection)
        yield self._add_btn
        yield self._launch_btn
        yield Button("Quit", id="quit")

    def on_mount(self):
        if self._add_basepath:
            self._add_directory(str(self._selected_dir))

    def _check_valid_selection(self, valid: bool):
        if self._add_btn:
            if valid:
                self._add_btn.disabled = False
            else:
                self._add_btn.disabled = True

    def _add_directory(self, directory: str):
        source = Path(self._dir_tree.path).resolve() / directory
        self.app._environment.sources.append(source)
        machine_data = requests.get(
            f"{self.app._environment.url.geturl()}/machine/"
        ).json()
        self.app._default_destinations[
            source
        ] = f"{machine_data.get('rsync_module') or 'data'}/{datetime.now().year}"
        if self._launch_btn:
            self._launch_btn.disabled = False
        self.query_one("#selected-directories").write(str(source) + "\n")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "quit":
            self.app.exit()
            exit()
        elif event.button.id == "add":
            self._add_directory(self._dir_tree._selected_path)
        elif event.button.id == "launch":
            text = self.app._visit
            visit_path = ""
            transfer_routes = {}
            for i, (s, defd) in enumerate(self.app._default_destinations.items()):
                _default = determine_default_destination(
                    self.app._visit, s, defd, self.app._environment, self.app.analysers
                )
                visit_path = defd + f"/{text}"
                if self.app._environment.processing_only_mode:
                    self.app._start_rsyncer(_default, visit_path=visit_path)
                transfer_routes[s] = _default
            self.app.install_screen(
                DestinationSelect(transfer_routes), "destination-select-screen"
            )
            self.app.pop_screen()
            self.app.push_screen("destination-select-screen")


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
                    if self.app.screen._name == "main":
                        break
                    self.app.pop_screen()
                except ScreenStackError:
                    break
            self.app.push_screen("main")
            self.app.uninstall_screen("confirm")
        if self._callback and event.button.id == "launch":
            self._callback(params=self._params)


class ProcessingForm(Screen):
    _form = reactive({})
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
            "dose_per_frame": "Dose Per Frame (e- / Angstrom^2 / frame)",
            "tilt_offset": "Tilt Offset",
            "file_extension": "File Extension",
            "acquisition_software": "Acquisition Software",
            "use_cryolo": "Use crYOLO Autopicking",
            "symmetry": "Symmetry Group",
            "boxsize": "Box Size",
            "eer_grouping": "EER Grouping",
            "mask_diameter": "Mask Diameter (2D classification)",
            "downscale": "Downscale Extracted Particles",
            "source": "Source Directory",
            "small_boxsize": "Downscaled Extracted Particle Size (pixels)",
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
                self.app.query_one("#info").write(
                    f"{self._readable_labels.get(k, k)}: {v}"
                )
            self.app._start_dc(params)

    def on_input_submitted(self, event):
        k = self._inputs[event.input]
        self._form[k] = event.value

    def on_button_pressed(self, event):
        if "confirm" not in self.app._installed_screens:
            self.app.install_screen(
                ConfirmScreen(
                    "Launch processing?",
                    params=self._form,
                    pressed_callback=self._write_params,
                ),
                "confirm",
            )
        self.app.push_screen("confirm")


class SwitchSelection(Screen):
    def __init__(
        self,
        name: str,
        elements: List[str],
        switch_label: str,
        switch_status: bool = True,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._elements = elements
        self._switch_status = switch_status
        self._switch_label = switch_label
        self._name = name

    def compose(self):
        hovers = (
            [
                Button(e, id=f"btn-{self._name}-{e}", classes=f"btn-{self._name}")
                for e in self._elements
            ]
            if self._elements
            else [Button("No elements found")]
        )
        yield Vertical(*hovers, id=f"select-{self._name}")
        yield Static(self._switch_label, id=f"label-{self._name}")
        yield Switch(id=f"switch-{self._name}", value=self._switch_status)

    def on_switch_changed(self, event):
        self._switch_status = event.value


class VisitSelection(SwitchSelection):
    def __init__(self, visits: List[str], *args, **kwargs):
        super().__init__("visit", visits, "Create visit directory", *args, **kwargs)

    def on_button_pressed(self, event: Button.Pressed):
        text = str(event.button.label)
        self.app._visit = text
        self.app._environment.visit = text
        if self._switch_status:
            machine_data = requests.get(
                f"{self.app._environment.url.geturl()}/machine/"
            ).json()
            self.app.install_screen(
                DirectorySelection(
                    [
                        p[0]
                        for p in machine_data.get("data_directories", {}).items()
                        if p[1] == "detector" and Path(p[0]).exists()
                    ]
                ),
                "directory-select",
            )
        self.app.pop_screen()
        if self._switch_status:
            self.app.push_screen("directory-select")
        else:
            self.app.install_screen(LaunchScreen(basepath=Path("./")), "launcher")
            self.app.push_screen("launcher")


class DirectorySelection(SwitchSelection):
    def __init__(self, directories: List[str], *args, **kwargs):
        super().__init__("directory", directories, "Multigrid", *args, **kwargs)

    def on_button_pressed(self, event: Button.Pressed):
        self.app._multigrid = self._switch_status
        visit_dir = Path(str(event.button.label)) / self.app._visit
        visit_dir.mkdir(exist_ok=True)
        self.app.install_screen(
            LaunchScreen(basepath=visit_dir, add_basepath=True), "launcher"
        )
        self.app.pop_screen()
        self.app.push_screen("launcher")


class DestinationSelect(Screen):
    def __init__(self, transfer_routes: Dict[Path, str], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._transfer_routes = transfer_routes
        self._dose_per_frame = None

    def compose(self):
        bulk = []
        for s, d in self._transfer_routes.items():
            bulk.append(Label(f"Copy the source {s} to:"))
            bulk.append(Input(value=d, classes="input-destination"))
        yield Vertical(*bulk, id="destination-holder")
        yield Vertical(
            Label("Dose per frame (e- / Angstrom^2 / frame)"),
            Input(id="dose", classes="input-destination"),
            id="dose-per-frame",
        )
        yield Button("Confirm", id="destination-btn")

    def on_input_changed(self, event):
        if event.input.id == "dose":
            try:
                self._dose_per_frame = float(event.value)
            except ValueError:
                pass

    def on_button_pressed(self, event):
        if self._dose_per_frame is None:
            return
        for s, d in self._transfer_routes.items():
            self.app._default_destinations[s] = d
            self.app._register_dc = True
            self.app._start_rsyncer(s, d)
        self.app._environment.data_collection_parameters[
            "dose_per_frame"
        ] = self._dose_per_frame
        self.app.pop_screen()
        self.app.push_screen("main")


class MainScreen(Screen):
    def compose(self):
        self.app.log_book = TextLog(id="log_book", wrap=True, max_lines=200)
        if self.app._redirected_logger:
            log.info("connecting logger")
            self.app._redirected_logger.text_log = self.app.log_book
            log.info("logger connected")
        self.app.hovers = (
            [Button(v, id="visit-btn") for v in self.app.visits]
            if len(self.app.visits)
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
        self.app.processing_form = ProcessingForm(self.app._form_values)
        yield Header()
        info_widget = TextLog(id="info", markup=True)
        yield info_widget
        yield self.app.log_book
        info_widget.write("[bold]Welcome to Murfey[/bold]")
        self.app.processing_btn = Button(
            "Request processing",
            id="processing-btn",
            disabled=not self.app._form_values,
        )
        yield self.app.processing_btn
        yield Button("Visit complete", id="new-visit-btn")
        yield Footer()
