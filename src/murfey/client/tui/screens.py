from __future__ import annotations

# import contextlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, NamedTuple, OrderedDict, Type, TypeVar

import procrunner
import requests
from pydantic import BaseModel, ValidationError
from rich.box import SQUARE
from rich.panel import Panel
from textual.app import ScreenStackError
from textual.containers import VerticalScroll
from textual.message import Message
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
    OptionList,
    Static,
    Switch,
    TextLog,
    Tree,
)

from murfey.client.analyser import Analyser, spa_form_dependencies
from murfey.client.context import SPAContext, TomographyContext
from murfey.client.gain_ref import determine_gain_ref
from murfey.client.instance_environment import (
    MurfeyInstanceEnvironment,
    global_env_lock,
)
from murfey.client.tui.forms import FormDependency
from murfey.util import get_machine_config
from murfey.util.models import DCParametersSPA, DCParametersTomo

log = logging.getLogger("murfey.tui.screens")

ReactiveType = TypeVar("ReactiveType")


def determine_default_destination(
    visit: str,
    source: Path,
    destination: str,
    environment: MurfeyInstanceEnvironment,
    analysers: Dict[Path, Analyser],
    touch: bool = False,
    extra_directory: str = "",
    include_mid_path: bool = True,
    use_suggested_path: bool = True,
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
                    if (
                        machine_data["data_directories"][data_dir] == "detector"
                        and use_suggested_path
                    ):
                        with global_env_lock:
                            source_name = (
                                source.name
                                if source.name != "Images-Disc1"
                                else source.parent.name
                            )
                            if environment.destination_registry.get(source_name):
                                _default = environment.destination_registry[source_name]
                            else:
                                suggested_path_response = requests.post(
                                    url=f"{str(environment.url.geturl())}/visits/{visit}/suggested_path",
                                    json={
                                        "base_path": f"{destination}/{visit}/{mid_path.parent if include_mid_path else ''}/raw",
                                        "touch": touch,
                                    },
                                )
                                _default = suggested_path_response.json().get(
                                    "suggested_path"
                                )
                                environment.destination_registry[source_name] = _default
                    else:
                        _default = f"{destination}/{visit}/{mid_path if include_mid_path else source.name}"
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
    return _default + f"/{extra_directory}"


class InputResponse(NamedTuple):
    question: str
    allowed_responses: List[str] | None = None
    default: str = ""
    callback: Callable | None = None
    key_change_callback: Callable | None = None
    kwargs: dict | None = None
    form: OrderedDict[str, Any] | None = None
    model: BaseModel | None = None


class LogBook(TextLog):
    class Log(Message):
        def __init__(self, log_renderable):
            self.renderable = log_renderable
            super().__init__()


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


def validate_form(form: dict, model: BaseModel) -> bool:
    try:
        convert = lambda x: None if x == "None" else x
        validated = model(**{k: convert(v) for k, v in form.items()})
        log.info(validated.dict())
        return True
    except (AttributeError, ValidationError) as e:
        log.warning(f"Form validation failed: {str(e)}")
        return False


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
            if not self._data_directories:
                self.valid_selection = True
                return
            for d in self._data_directories:
                if Path(self._selected_path).resolve().is_relative_to(d):
                    self.valid_selection = True
                    break
            else:
                self.valid_selection = False
        else:
            self.valid_selection = False


class _DirectoryTreeGain(DirectoryTree):
    valid_selection = reactive(False)

    def __init__(self, gain_reference: Path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._gain_reference = gain_reference

    def on_tree_node_selected(self, event: Tree.NodeSelected) -> None:
        event.stop()
        dir_entry = event.node.data
        if dir_entry is None:
            return
        if not dir_entry.is_dir:
            self.valid_selection = True
            self._gain_reference = Path(dir_entry.path)
        else:
            self.valid_selection = False


class LaunchScreen(Screen):
    _launch_btn: Button | None = None

    def __init__(
        self, *args, basepath: Path = Path("."), add_basepath: bool = False, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self._selected_dir = basepath
        self._add_basepath = add_basepath
        self._context = SPAContext

    def compose(self):
        machine_data = requests.get(
            f"{self.app._environment.url.geturl()}/machine/"
        ).json()
        self._dir_tree = _DirectoryTree(
            str(self._selected_dir),
            data_directories=machine_data.get("data_directories", {})
            if self.app._strict
            else {},
            id="dir-select",
        )

        yield self._dir_tree
        text_log = TextLog(id="selected-directories")
        widgets = [text_log, Button("Clear", id="clear")]
        if self.app._multigrid:
            widgets.append(Label("Data collection modality:"))
            widgets.append(OptionList("SPA", "Tomography", id="modality-select"))
        text_log_block = VerticalScroll(*widgets, id="selected-directories-vert")
        yield text_log_block

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

    def _add_directory(self, directory: str, add_destination: bool = True):
        source = Path(self._dir_tree.path).resolve() / directory
        if add_destination:
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

    def on_option_list_option_selected(self, event):
        log.info(f"option selected: {event.option}")
        if event.option.prompt == "Tomography":
            log.info("switching context to tomo")
            self._context = TomographyContext
        elif event.option.prompt == "SPA":
            self._context = SPAContext

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
            for s, defd in self.app._default_destinations.items():
                _default = determine_default_destination(
                    self.app._visit,
                    s,
                    defd,
                    self.app._environment,
                    self.app.analysers,
                )
                visit_path = defd + f"/{text}"
                if self.app._environment.processing_only_mode:
                    self.app._start_rsyncer(_default, visit_path=visit_path)
                transfer_routes[s] = _default
            self.app.install_screen(
                DestinationSelect(
                    transfer_routes, self._context, dependencies=spa_form_dependencies
                ),
                "destination-select-screen",
            )
            self.app.pop_screen()
            self.app.push_screen("destination-select-screen")
        elif event.button.id == "clear":
            sel_dir = self.query_one("#selected-directories")
            for line in sel_dir.lines[1:]:
                source = Path(line.text)
                if source in self.app._environment.sources:
                    self.app._environment.sources.remove(source)
                    if self.app._default_destinations.get(source):
                        del self.app._default_destinations[source]
            sel_dir.clear()
            sel_dir.write("Selected directories:\n")


class ConfirmScreen(Screen):
    def __init__(
        self,
        prompt: str,
        *args,
        params: dict | None = None,
        pressed_callback: Callable | None = None,
        button_names: dict | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._prompt = prompt
        self._params = params or {}
        self._callback = pressed_callback
        self._button_names = button_names or {}

    def compose(self):
        if self._params:
            dt = DataTable(id="prompt")
            keys = list(self._params.keys())
            dt.add_columns(*keys)
            dt.add_rows([[self._params[k] for k in keys]])
            yield dt
        else:
            yield Static(self._prompt, id="prompt")
        yield Button(self._button_names.get("launch") or "Launch", id="launch")
        yield Button(self._button_names.get("quit") or "Back", id="quit")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "quit":
            self.app.pop_screen()
            self.app.uninstall_screen("confirm")
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

    def __init__(
        self,
        form: dict,
        *args,
        dependencies: Dict[str, FormDependency] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._form = form
        self._inputs: Dict[Input, str] = {}
        self._dependencies = dependencies or {}

    def compose(self):
        inputs = []
        analyser = list(self.app.analysers.values())[0]
        for k in analyser._context.user_params + analyser._context.metadata_params:
            t = k.label
            inputs.append(Label(t, classes="label"))
            if self._form.get(k.name) in ("true", "True", True):
                i = Switch(value=True, classes="input", id=f"switch_{k.name}")
            elif self._form.get(k.name) in ("false", "False", False):
                i = Switch(value=False, classes="input", id=f"switch_{k.name}")
            else:
                i = Input(placeholder=t, classes="input", id=f"input_{k.name}")
                default = self._form.get(k.name)
                i.value = "None" if default is None else default
            self._inputs[i] = k.name
            inputs.append(i)
        for i, k in self._inputs.items():
            self._check_dependency(k, i.value)
        confirm_btn = Button("Confirm", id="confirm-btn")
        if self._form.get("motion_corr_binning") == "2":
            self._vert = VerticalScroll(
                *inputs,
                Label("Collected in super resoultion mode unbinned:"),
                Switch(id="superres", value=True, classes="input"),
                confirm_btn,
                id="input-form",
            )
        else:
            self._vert = VerticalScroll(*inputs, confirm_btn, id="input-form")
        yield self._vert

    def _write_params(self, params: dict | None = None):
        if params:
            analyser = list(self.app.analysers.values())[0]
            for k in analyser._context.user_params + analyser._context.metadata_params:
                self.app.query_one("#info").write(f"{k.label}: {params.get(k.name)}")
            self.app._start_dc(params)

    def on_switch_changed(self, event):
        if event.switch.id == "superres":
            pix_size = self.query_one("#input_pixel_size_on_image")
            motion_corr_binning = self.query_one("#input_motion_corr_binning")
            if event.value:
                pix_size.value = str(float(pix_size.value) / 2)
                motion_corr_binning.value = "2"
            else:
                pix_size.value = str(float(pix_size.value) * 2)
                motion_corr_binning.value = "1"
        else:
            k = self._inputs[event.switch]
            self._form[k] = event.value
            self._check_dependency(k, event.value)

    def _check_dependency(self, key: str, value: Any):
        if x := self._dependencies.get(key):
            for d, v in x.dependencies.items():
                if value == x.trigger_value:
                    self._form[d] = v
                    for i, dk in self._inputs.items():
                        if dk == d:
                            i.value = v
                            i.disabled = True
                            break
                else:
                    for i, dk in self._inputs.items():
                        if dk == d:
                            i.disabled = False
                            break

    def on_input_changed(self, event):
        k = self._inputs[event.input]
        self._form[k] = event.value

    def on_button_pressed(self, event):
        if self.app.analysers.get(Path(self._form.get("source", ""))):
            if model := self.app.analysers[Path(self._form["source"])].parameters_model:
                valid = validate_form(self._form, model)
                if not valid:
                    return
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
        yield VerticalScroll(*hovers, id=f"select-{self._name}")
        yield Static(self._switch_label, id=f"label-{self._name}")
        yield Switch(id=f"switch-{self._name}", value=self._switch_status)

    def on_switch_changed(self, event):
        self._switch_status = event.value


class VisitSelection(SwitchSelection):
    def __init__(self, visits: List[str], *args, **kwargs):
        super().__init__(
            "visit",
            visits,
            "Create visit directory (if you have already started an acquisiton session disable this)",
            *args,
            **kwargs,
        )

    def on_button_pressed(self, event: Button.Pressed):
        text = str(event.button.label)
        self.app._visit = text
        self.app._environment.visit = text
        machine_data = requests.get(
            f"{self.app._environment.url.geturl()}/machine/"
        ).json()
        if self._switch_status:
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

        if machine_data.get("gain_reference_directory"):
            self.app.install_screen(
                GainReference(
                    determine_gain_ref(Path(machine_data["gain_reference_directory"])),
                    self._switch_status,
                ),
                "gain-ref-select",
            )
            self.app.push_screen("gain-ref-select")
        else:
            if self._switch_status:
                self.app.push_screen("directory-select")
            else:
                self.app.install_screen(LaunchScreen(basepath=Path("./")), "launcher")
                self.app.push_screen("launcher")


class GainReference(Screen):
    def __init__(self, gain_reference: Path, switch_status: bool, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._gain_reference = gain_reference
        self._switch_status = switch_status

    def compose(self):
        self._dir_tree = _DirectoryTreeGain(
            self._gain_reference,
            str(self._gain_reference.parent.parent),
            id="gain-select",
        )
        yield self._dir_tree
        self._launch_btn = Button("Launch", id="launch")
        self.watch(self._dir_tree, "valid_selection", self._check_valid_selection)
        yield self._launch_btn
        yield Button("No gain", id="skip-gain")

    def _check_valid_selection(self, valid: bool):
        if self._launch_btn:
            if valid:
                self._launch_btn.disabled = False
            else:
                self._launch_btn.disabled = True

    def on_button_pressed(self, event):
        if event.button.id == "skip-gain":
            self.app.pop_screen()
        else:
            visit_path = f"data/{datetime.now().year}/{self.app._environment.visit}"
            cmd = [
                "rsync",
                str(self._dir_tree._gain_reference),
                f"{self.app._environment.url.hostname}::{visit_path}/processing",
            ]
            if self.app._environment.demo:
                log.info(f"Would perform {' '.join(cmd)}")
            else:
                gain_rsync = procrunner.run(cmd)
                if gain_rsync.returncode:
                    log.warning(
                        f"Gain reference file {self._dir_tree._gain_reference} was not successfully transferred to {visit_path}/processing"
                    )
            process_gain_response = requests.post(
                url=f"{str(self.app._environment.url.geturl())}/visits/{self.app._environment.visit}/process_gain",
                json={
                    "gain_ref": str(self._dir_tree._gain_reference),
                },
            )
            if str(process_gain_response.status_code).startswith("4"):
                log.warning(
                    f"Gain processing failed: status code {process_gain_response.status_code}"
                )
            else:
                log.info(
                    f"Gain reference file {process_gain_response.json().get('gain_ref')}"
                )
                self.app._environment.data_collection_parameters[
                    "gain_ref"
                ] = process_gain_response.json().get("gain_ref")
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
        (visit_dir / "atlas").mkdir(exist_ok=True)
        self.app.install_screen(
            LaunchScreen(basepath=visit_dir, add_basepath=True), "launcher"
        )
        self.app.pop_screen()
        self.app.push_screen("launcher")


class DestinationSelect(Screen):
    def __init__(
        self,
        transfer_routes: Dict[Path, str],
        context: Type[SPAContext] | Type[TomographyContext],
        *args,
        dependencies: Dict[str, FormDependency] | None = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self._transfer_routes = transfer_routes
        self._destination_overrides: Dict[Path, str] = {}
        self._user_params: Dict[str, str] = {}
        self._dependencies = dependencies or {}
        self._inputs: Dict[Input, str] = {}
        self._context = context

    def compose(self):
        bulk = []
        if self.app._multigrid:
            for s in self._transfer_routes.keys():
                for d in s.glob("*"):
                    if d.is_dir() and d.name != "atlas":
                        machine_data = requests.get(
                            f"{self.app._environment.url.geturl()}/machine/"
                        ).json()
                        dest = determine_default_destination(
                            self.app._visit,
                            s,
                            f"{machine_data.get('rsync_module') or 'data'}/{datetime.now().year}",
                            self.app._environment,
                            self.app.analysers,
                            touch=True,
                        )
                        bulk.append(Label(f"Copy the source {d} to:"))
                        bulk.append(
                            Input(
                                value=dest,
                                id=f"destination-{str(d)}",
                                classes="input-destination",
                            )
                        )
        else:
            for s, d in self._transfer_routes.items():
                bulk.append(Label(f"Copy the source {s} to:"))
                bulk.append(
                    Input(
                        value=d, id=f"destination-{str(s)}", classes="input-destination"
                    )
                )
        yield VerticalScroll(*bulk, id="destination-holder")
        params_bulk = []
        if self.app._multigrid:
            for k in self._context.user_params:
                params_bulk.append(Label(k.label))
                val = self.app._environment.data_collection_parameters.get(
                    k.name
                ) or str(k.default)
                self._user_params[k.name] = val
                if val in ("true", "True", True):
                    i = Switch(value=True, id=k.name, classes="input-destination")
                elif val in ("false", "False", False):
                    i = Switch(value=False, id=k.name, classes="input-destination")
                else:
                    i = Input(value=val, id=k.name, classes="input-destination")
                params_bulk.append(i)
                self._inputs[i] = k.name
            machine_config = get_machine_config(
                str(self.app._environment.url.geturl()), demo=self.app._environment.demo
            )
            if machine_config.get("superres"):
                params_bulk.append(
                    Label("Collected in super resoultion mode unbinned:")
                )
                params_bulk.append(
                    Switch(
                        value=False,
                        id="superres-multigrid",
                        classes="input-destination",
                    )
                )
                self.app._environment.superres = False
            for i, k in self._inputs.items():
                self._check_dependency(k, i.value)
        yield VerticalScroll(
            *params_bulk,
            id="user-params",
        )
        yield Button("Confirm", id="destination-btn")

    def _check_dependency(self, key: str, value: Any):
        if x := self._dependencies.get(key):
            for d, v in x.dependencies.items():
                if value == x.trigger_value:
                    self._user_params[d] = str(v)
                    for i, dk in self._inputs.items():
                        if dk == d:
                            i.value = v
                            i.disabled = True
                            break
                else:
                    for i, dk in self._inputs.items():
                        if dk == d:
                            i.disabled = False
                            break

    def on_switch_changed(self, event):
        if event.switch.id == "superres-multigrid":
            self.app._environment.superres = event.value
        else:
            for k in self._context.user_params:
                if event.switch.id == k.name:
                    self._user_params[k.name] = event.value
                    self._check_dependency(k.name, event.value)

    def on_input_changed(self, event):
        if event.input.id.startswith("destination-"):
            if not self.app._multigrid:
                self._transfer_routes[Path(event.input.id[12:])] = event.value
            else:
                self._destination_overrides[Path(event.input.id[12:])] = event.value
        else:
            for k in self._context.user_params:
                if event.input.id == k.name:
                    self._user_params[k.name] = event.value

    def on_button_pressed(self, event):
        if self.app._multigrid:
            if self._context == TomographyContext:
                valid = validate_form(self._user_params, DCParametersTomo.Base)
            else:
                valid = validate_form(self._user_params, DCParametersSPA.Base)
            if not valid:
                return
        for s, d in self._transfer_routes.items():
            self.app._default_destinations[s] = d
            self.app._register_dc = True
            if self.app._multigrid:
                for k, v in self._destination_overrides.items():
                    self.app._environment.destination_registry[k.name] = v
                self.app._launch_multigrid_watcher(
                    s, destination_overrides=self._destination_overrides
                )
            else:
                self.app._start_rsyncer(s, d)
        for k, v in self._user_params.items():
            self.app._environment.data_collection_parameters[k] = v
        if len(self._transfer_routes) > 1:
            requests.post(
                f"{self.app._environment.url.geturl()}/visits/{self.app._environment.visit}/write_connections_file",
                json={
                    "filename": f"murfey-{datetime.now().strftime('%Y-%m-%d-%H_%M_%S')}.txt",
                    "destinations": [
                        Path(n).name for n in self._transfer_routes.values()
                    ],
                },
            )
        self.app.pop_screen()
        self.app.push_screen("main")


class MainScreen(Screen):
    def compose(self):
        self.app.log_book = LogBook(id="log_book", wrap=True, max_lines=200)
        if self.app._redirected_logger:
            log.info("connecting logger")
            self.app._redirected_logger.text_log = self.app.log_book
            log.info("logger connected")
        self.app.hovers = (
            [Button(v, id="visit-btn") for v in self.app.visits]
            if len(self.app.visits)
            else [Button("No ongoing visits found")]
        )
        self.app.processing_form = ProcessingForm(
            self.app._form_values, dependencies=self.app._form_dependencies
        )
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
