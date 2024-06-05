from __future__ import annotations

import logging
from datetime import datetime
from functools import partial
from pathlib import Path
from queue import Queue
from typing import Awaitable, Callable, Dict, List, OrderedDict, TypeVar
from urllib.parse import urlparse

import procrunner
import requests
from textual.app import App
from textual.reactive import reactive
from textual.widgets import Button, Input

from murfey.client.analyser import Analyser
from murfey.client.contexts.spa import SPAContext, SPAModularContext
from murfey.client.contexts.tomo import TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncer, RSyncerUpdate, TransferResult
from murfey.client.tui.screens import (
    ConfirmScreen,
    InputResponse,
    MainScreen,
    ProcessingForm,
    SessionSelection,
    VisitSelection,
    WaitingScreen,
    determine_default_destination,
)
from murfey.client.tui.status_bar import StatusBar
from murfey.client.watchdir import DirWatcher
from murfey.client.watchdir_multigrid import MultigridDirWatcher
from murfey.util import (
    capture_post,
    get_machine_config,
    read_config,
    set_default_acquisition_output,
)

log = logging.getLogger("murfey.tui.app")

ReactiveType = TypeVar("ReactiveType")

token = read_config()["Murfey"].get("token", "")

requests.get = partial(requests.get, headers={"Authorization": f"Bearer {token}"})
requests.post = partial(requests.post, headers={"Authorization": f"Bearer {token}"})
requests.delete = partial(requests.delete, headers={"Authorization": f"Bearer {token}"})


class MurfeyTUI(App):
    CSS_PATH = "controller.css"
    processing_btn: Button
    processing_form: ProcessingForm
    hover: List[str]
    visits: List[str]
    rsync_processes: Dict[Path, RSyncer] = {}
    analysers: Dict[Path, Analyser] = {}
    _form_values: dict = reactive({})
    _form_dependencies: dict = {}

    def __init__(
        self,
        environment: MurfeyInstanceEnvironment | None = None,
        visits: List[str] | None = None,
        queues: Dict[str, Queue] | None = None,
        status_bar: StatusBar | None = None,
        dummy_dc: bool = True,
        do_transfer: bool = True,
        gain_ref: Path | None = None,
        redirected_logger=None,
        force_mdoc_metadata: bool = False,
        strict: bool = False,
        processing_enabled: bool = True,
        skip_existing_processing: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._environment = environment or MurfeyInstanceEnvironment(
            urlparse("http://localhost:8000")
        )
        self._environment.gain_ref = gain_ref
        self._sources = self._environment.sources or [Path(".")]
        self._url = self._environment.url
        self._default_destinations = self._environment.default_destinations
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
        self._data_collection_form_complete = False
        self._form_readable_labels: dict = {}
        self._redirected_logger = redirected_logger
        self._multigrid = False
        self._processing_enabled = processing_enabled
        self._multigrid_watcher: MultigridDirWatcher | None = None
        self._force_mdoc_metadata = force_mdoc_metadata
        self._strict = strict
        self._skip_existing_processing = skip_existing_processing
        self._machine_config = get_machine_config(
            str(self._environment.url.geturl()), demo=self._environment.demo
        )
        self._data_suffixes = (".mrc", ".tiff", ".tif", ".eer")
        self._data_substrings = [
            s
            for val in self._machine_config["data_required_substrings"].values()
            for ds in val.values()
            for s in ds
        ]
        self.install_screen(MainScreen(), "main")

    @property
    def role(self) -> str:
        if self.analyser:
            return self.analyser._role
        return ""

    def _launch_multigrid_watcher(
        self, source: Path, destination_overrides: Dict[Path, str] | None = None
    ):
        log.info(f"Launching multigrid watcher for source {source}")
        machine_config = get_machine_config(
            str(self._environment.url.geturl()), demo=self._environment.demo
        )
        self._multigrid_watcher = MultigridDirWatcher(
            source,
            machine_config,
            skip_existing_processing=self._skip_existing_processing,
        )
        self._multigrid_watcher.subscribe(
            partial(
                self._start_rsyncer_multigrid,
                destination_overrides=destination_overrides or {},
            )
        )
        self._multigrid_watcher.start()

    def _start_rsyncer_multigrid(
        self,
        source: Path,
        extra_directory: str = "",
        include_mid_path: bool = True,
        use_suggested_path: bool = True,
        destination_overrides: Dict[Path, str] | None = None,
        remove_files: bool = False,
        analyse: bool = True,
        limited: bool = False,
    ):
        log.info(f"starting multigrid rsyncer: {source}")
        destination_overrides = destination_overrides or {}
        machine_data = requests.get(f"{self._environment.url.geturl()}/machine/").json()
        if destination_overrides.get(source):
            destination = destination_overrides[source] + f"/{extra_directory}"
        else:
            for k, v in destination_overrides.items():
                if Path(v).name in source.parts:
                    destination = str(k / extra_directory)
                    break
            else:
                self._environment.default_destinations[source] = (
                    f"{machine_data.get('rsync_module') or 'data'}/{datetime.now().year}"
                )
                destination = determine_default_destination(
                    self._visit,
                    source,
                    self._default_destinations[source],
                    self._environment,
                    self.analysers,
                    touch=True,
                    extra_directory=extra_directory,
                    include_mid_path=include_mid_path,
                    use_suggested_path=use_suggested_path,
                )
        self._environment.sources.append(source)
        self._start_rsyncer(
            source,
            destination,
            force_metadata=self._processing_enabled,
            # analyse=not extra_directory and use_suggested_path and analyse,
            analyse=analyse,
            remove_files=remove_files,
            limited=limited,
        )

    def _start_rsyncer(
        self,
        source: Path,
        destination: str,
        visit_path: str = "",
        force_metadata: bool = False,
        analyse: bool = True,
        remove_files: bool = False,
        limited: bool = False,
    ):
        log.info(f"starting rsyncer: {source}")
        if self._environment:
            self._environment.default_destinations[source] = destination
            if self._environment.gain_ref and visit_path:
                gain_rsync = procrunner.run(
                    [
                        "rsync",
                        str(self._environment.gain_ref),
                        f"{self._url.hostname}::{visit_path}/processing",
                    ]
                )
                if gain_rsync.returncode:
                    log.warning(
                        f"Gain reference file {self._environment.gain_ref} was not successfully transferred to {visit_path}/processing"
                    )
        self.rsync_processes[source] = RSyncer(
            source,
            basepath_remote=Path(destination),
            server_url=self._url,
            # local=self._environment.demo,
            status_bar=self._statusbar,
            do_transfer=self._do_transfer,
            required_substrings_for_removal=self._data_substrings,
            remove_files=remove_files,
        )

        def rsync_result(update: RSyncerUpdate):
            if not update.base_path:
                raise ValueError("No base path from rsyncer update")
            if not self.rsync_processes.get(update.base_path):
                raise ValueError("TUI rsync process does not exist")
            if update.outcome is TransferResult.SUCCESS:
                # log.info(
                #     f"File {str(update.file_path)!r} successfully transferred ({update.file_size} bytes)"
                # )
                pass
            else:
                log.warning(f"Failed to transfer file {str(update.file_path)!r}")
                self.rsync_processes[update.base_path].enqueue(update.file_path)

        self.rsync_processes[source].subscribe(rsync_result)
        self.rsync_processes[source].subscribe(
            partial(
                self._increment_transferred_files_prometheus,
                destination=destination,
                source=str(source),
            )
        )
        self.rsync_processes[source].subscribe(
            partial(
                self._increment_transferred_files,
                destination=destination,
                source=str(source),
            ),
            secondary=True,
        )
        url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/rsyncer"
        rsyncer_data = {
            "source": str(source),
            "destination": destination,
            "client_id": self._environment.client_id,
            "transferring": self._do_transfer,
        }
        requests.post(url, json=rsyncer_data)
        self._environment.watchers[source] = DirWatcher(source, settling_time=30)

        if not self.analysers.get(source) and analyse:
            log.info(f"Starting analyser for {source}")
            self.analysers[source] = Analyser(
                source,
                environment=self._environment if not self._dummy_dc else None,
                force_mdoc_metadata=self._force_mdoc_metadata,
                limited=limited,
            )
            machine_data = requests.get(
                f"{self._environment.url.geturl()}/machine/"
            ).json()
            for data_dir in machine_data["data_directories"].keys():
                if source.resolve().is_relative_to(Path(data_dir)):
                    self.analysers[source]._role = machine_data["data_directories"][
                        data_dir
                    ]
                    log.info(f"role found for {source}")
                    break
            if force_metadata:
                self.analysers[source].subscribe(
                    partial(self._start_dc, from_form=True)
                )
            else:
                self.analysers[source].subscribe(self._data_collection_form)
            self.analysers[source].start()
            self.rsync_processes[source].subscribe(self.analysers[source].enqueue)

        self.rsync_processes[source].start()

        if self._environment:
            if self._environment.watchers.get(source):
                self._environment.watchers[source].subscribe(
                    self.rsync_processes[source].enqueue
                )
                self._environment.watchers[source].subscribe(
                    partial(
                        self._increment_file_count,
                        destination=destination,
                        source=str(source),
                    ),
                    secondary=True,
                )
                self._environment.watchers[source].start()

    def _increment_file_count(
        self, observed_files: List[Path], source: str, destination: str
    ):
        if len(observed_files):
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/increment_rsync_file_count"
            num_data_files = len(
                [
                    f
                    for f in observed_files
                    if f.suffix in self._data_suffixes
                    and any(substring in f.name for substring in self._data_substrings)
                ]
            )
            data = {
                "source": source,
                "destination": destination,
                "client_id": self._environment.client_id,
                "increment_count": len(observed_files),
                "increment_data_count": num_data_files,
            }
            requests.post(url, json=data)

    # Prometheus can handle higher traffic so update for every transferred file rather
    # than batching as we do for the Murfey database updates in _increment_transferred_files
    def _increment_transferred_files_prometheus(
        self, update: RSyncerUpdate, source: str, destination: str
    ):
        if update.outcome is TransferResult.SUCCESS:
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/increment_rsync_transferred_files_prometheus"
            data_files = (
                [update]
                if update.file_path.suffix in self._data_suffixes
                and any(
                    substring in update.file_path.name
                    for substring in self._data_substrings
                )
                else []
            )
            data = {
                "source": source,
                "destination": destination,
                "client_id": self._environment.client_id,
                "increment_count": 1,
                "bytes": update.file_size,
                "increment_data_count": len(data_files),
                "data_bytes": sum(f.file_size for f in data_files),
            }
            requests.post(url, json=data)

    def _increment_transferred_files(
        self, updates: List[RSyncerUpdate], source: str, destination: str
    ):
        checked_updates = [
            update for update in updates if update.outcome is TransferResult.SUCCESS
        ]
        if not checked_updates:
            return
        url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/increment_rsync_transferred_files"
        data_files = [
            u
            for u in updates
            if u.file_path.suffix in self._data_suffixes
            and any(
                substring in u.file_path.name for substring in self._data_substrings
            )
        ]
        data = {
            "source": source,
            "destination": destination,
            "client_id": self._environment.client_id,
            "increment_count": len(checked_updates),
            "bytes": sum(f.file_size for f in checked_updates),
            "increment_data_count": len(data_files),
            "data_bytes": sum(f.file_size for f in data_files),
        }
        requests.post(url, json=data)

    def _set_register_dc(self, response: str):
        if response == "y":
            self._register_dc = True
            for r in self._tmp_responses:
                self._queues["input"].put_nowait(
                    InputResponse(
                        question="Data collection parameters:",
                        form=r.get("form", OrderedDict({})),
                        model=getattr(self.analyser, "parameters_model", None),
                        callback=self.app._start_dc_confirm_prompt,
                    )
                )
                self._dc_metadata = r.get("form", OrderedDict({}))
        elif response == "n":
            self._register_dc = False
        self._tmp_responses = []

    def _data_collection_form(self, response: dict):
        log.info("data collection form ready")
        if self._data_collection_form_complete:
            return
        if self._register_dc and response.get("form"):
            self._form_values = {k: str(v) for k, v in response.get("form", {}).items()}
            log.info(
                f"gain reference is set to {self._form_values.get('gain_ref')}, {self._environment.data_collection_parameters.get('gain_ref')}"
            )
            if self._form_values.get("gain_ref") in (None, "None"):
                self._form_values["gain_ref"] = (
                    self._environment.data_collection_parameters.get("gain_ref")
                )
            self._form_dependencies = response.get("dependencies", {})
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

    def _start_dc(self, json, from_form: bool = False):
        if self._dummy_dc:
            return
        # for multigrid the analyser sends the message straight to _start_dc by-passing user input
        # it is then necessary to extract the data from the message
        if from_form:
            json = json.get("form", {})
            json = {k: v if v is None else str(v) for k, v in json.items()}
        self._environment.data_collection_parameters = {
            k: None if v == "None" else v for k, v in json.items()
        }
        source = Path(json["source"])
        context = self.analysers[source]._context
        if isinstance(context, TomographyContext):
            source = Path(json["source"])
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/{self._environment.client_id}/register_data_collection_group"
            dcg_data = {
                "experiment_type": "tomo",
                "experiment_type_id": 36,
                "tag": str(source),
                "atlas": (
                    str(self._environment.samples[source].atlas)
                    if self._environment.samples.get(source)
                    else ""
                ),
                "sample": (
                    self._environment.samples[source].sample
                    if self._environment.samples.get(source)
                    else None
                ),
            }
            capture_post(url, json=dcg_data)
            data = {
                "voltage": json["voltage"],
                "pixel_size_on_image": json["pixel_size_on_image"],
                "experiment_type": json["experiment_type"],
                "image_size_x": json["image_size_x"],
                "image_size_y": json["image_size_y"],
                "file_extension": json["file_extension"],
                "acquisition_software": json["acquisition_software"],
                "image_directory": str(self._environment.default_destinations[source]),
                "tag": json["tilt_series_tag"],
                "source": str(source),
                "magnification": json["magnification"],
                "total_exposed_dose": json.get("total_exposed_dose"),
                "c2aperture": json.get("c2aperture"),
                "exposure_time": json.get("exposure_time"),
                "slit_width": json.get("slit_width"),
                "phase_plate": json.get("phase_plate", False),
            }
            capture_post(
                f"{str(self._url.geturl())}/visits/{str(self._visit)}/{self._environment.client_id}/start_data_collection",
                json=data,
            )
            for recipe in ("em-tomo-preprocess", "em-tomo-align"):
                capture_post(
                    f"{str(self._url.geturl())}/visits/{str(self._visit)}/{self._environment.client_id}/register_processing_job",
                    json={"tag": json["tilt_series_tag"], "recipe": recipe},
                )
            log.info("Registering tomography processing parameters")
            if self.app._environment.data_collection_parameters.get("num_eer_frames"):
                eer_response = requests.post(
                    f"{str(self.app._environment.url.geturl())}/visits/{self.app._environment.visit}/eer_fractionation_file",
                    json={
                        "num_frames": self.app._environment.data_collection_parameters[
                            "num_eer_frames"
                        ],
                        "fractionation": self.app._environment.data_collection_parameters[
                            "eer_fractionation"
                        ],
                        "dose_per_frame": self.app._environment.data_collection_parameters[
                            "dose_per_frame"
                        ],
                        "fractionation_file_name": "eer_fractionation_tomo.txt",
                    },
                )
                eer_fractionation_file = eer_response.json()["eer_fractionation_file"]
                json.update({"eer_fractionation_file": eer_fractionation_file})
            requests.post(
                f"{self.app._environment.url.geturl()}/clients/{self.app._environment.client_id}/tomography_preprocessing_parameters",
                json=json,
            )
            context._flush_data_collections()
            context._flush_processing_jobs()
            capture_post(
                f"{self.app._environment.url.geturl()}/visits/{self._visit}/{self.app._environment.client_id}/flush_tomography_processing",
                json={"rsync_source": str(source)},
            )
            log.info("tomography processing flushed")
        elif isinstance(context, SPAContext) or isinstance(context, SPAModularContext):
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/{self._environment.client_id}/register_data_collection_group"
            dcg_data = {
                "experiment_type": "single particle",
                "experiment_type_id": 37,
                "tag": str(source),
                "atlas": (
                    str(self._environment.samples[source].atlas)
                    if self._environment.samples.get(source)
                    else ""
                ),
                "sample": (
                    self._environment.samples[source].sample
                    if self._environment.samples.get(source)
                    else None
                ),
            }
            capture_post(url, json=dcg_data)
            if from_form:
                data = {
                    "voltage": json["voltage"],
                    "pixel_size_on_image": json["pixel_size_on_image"],
                    "experiment_type": json["experiment_type"],
                    "image_size_x": json["image_size_x"],
                    "image_size_y": json["image_size_y"],
                    "file_extension": json["file_extension"],
                    "acquisition_software": json["acquisition_software"],
                    "image_directory": str(
                        self._environment.default_destinations[source]
                    ),
                    "tag": str(source),
                    "source": str(source),
                    "magnification": json["magnification"],
                    "total_exposed_dose": json.get("total_exposed_dose"),
                    "c2aperture": json.get("c2aperture"),
                    "exposure_time": json.get("exposure_time"),
                    "slit_width": json.get("slit_width"),
                    "phase_plate": json.get("phase_plate", False),
                }
                capture_post(
                    f"{str(self._url.geturl())}/visits/{str(self._visit)}/{self._environment.client_id}/start_data_collection",
                    json=data,
                )
                for recipe in (
                    "em-spa-preprocess",
                    "em-spa-extract",
                    "em-spa-class2d",
                    "em-spa-class3d",
                    "em-spa-refine",
                ):
                    capture_post(
                        f"{str(self._url.geturl())}/visits/{str(self._visit)}/{self._environment.client_id}/register_processing_job",
                        json={"tag": str(source), "recipe": recipe},
                    )
                log.info(f"Posting SPA processing parameters: {json}")
                response = capture_post(
                    f"{self.app._environment.url.geturl()}/clients/{self.app._environment.client_id}/spa_processing_parameters",
                    json={
                        **{k: None if v == "None" else v for k, v in json.items()},
                        "tag": str(source),
                    },
                )
                if response is None:
                    log.error(
                        "Could not reach Murfey server to insert SPA processing parameters"
                    )
                    return None
                if not str(response.status_code).startswith("2"):
                    log.warning(f"{response.reason}")
                capture_post(
                    f"{self.app._environment.url.geturl()}/visits/{self.app._environment.visit}/{self.app._environment.client_id}/flush_spa_processing",
                    json={"tag": str(source)},
                )
            if isinstance(context, SPAContext):
                url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/{self._environment.client_id}/start_data_collection"
                self._environment.listeners["data_collection_group_ids"] = {
                    partial(
                        context._register_data_collection,
                        url=url,
                        data=json,
                        environment=self._environment,
                    )
                }
                self._environment.listeners["data_collection_ids"] = {
                    partial(
                        context._register_processing_job,
                        parameters=json,
                        environment=self._environment,
                    )
                }
                url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/spa_processing"
                self._environment.listeners["processing_job_ids"] = {
                    partial(
                        context._launch_spa_pipeline,
                        url=url,
                        environment=self._environment,
                    )
                }

    def _set_request_destination(self, response: str):
        if response == "y":
            self._request_destinations = True

    async def on_load(self, event):
        self.bind("q", "quit", description="Quit", show=True)
        self.bind("p", "process", description="Allow processing", show=True)
        self.bind(
            "d", "remove_session", description="Quit and remove session", show=True
        )

    def _install_processing_form(self):
        self.processing_form = ProcessingForm(
            self._form_values, dependencies=self._form_dependencies
        )
        self.install_screen(self.processing_form, "processing-form")

    def on_input_submitted(self, event: Input.Submitted):
        event.input.has_focus = False
        self.screen.focused = None

    async def on_button_pressed(self, event: Button.Pressed):
        if event.button._id == "processing-btn":
            self._install_processing_form()
            self.push_screen("processing-form")
        elif event.button._id == "new-visit-btn":
            await self.reset()

    async def on_mount(self) -> None:
        exisiting_sessions = requests.get(
            f"{self._environment.url.geturl()}/sessions"
        ).json()
        self.install_screen(VisitSelection(self.visits), "visit-select-screen")
        self.push_screen("visit-select-screen")
        if exisiting_sessions:
            self.install_screen(
                SessionSelection(
                    [
                        f"{s['session']['id']}: {s['session']['name']}"
                        for s in exisiting_sessions
                    ],
                    [
                        f"{s['session']['id']}: {s['session']['name']}"
                        for s in exisiting_sessions
                        if s["clients"]
                    ],
                ),
                "session-select-screen",
            )
            self.push_screen("session-select-screen")
        else:
            session_name = "Client connection"
            resp = capture_post(
                f"{self._environment.url.geturl()}/clients/{self._environment.client_id}/session",
                json={"session_id": None, "session_name": session_name},
            )
            if resp:
                self._environment.murfey_session = resp.json()

    def on_log_book_log(self, message):
        self.log_book.write(message.renderable)

    async def reset(self):
        machine_config = get_machine_config(
            str(self._environment.url.geturl()), demo=self._environment.demo
        )
        if self.rsync_processes and machine_config.get("allow_removal"):
            sources = "\n".join(str(k) for k in self.rsync_processes.keys())
            prompt = f"Remove files from the following:\n {sources} \n"
            rsync_instances = requests.get(
                f"{self._environment.url.geturl()}/clients/{self._environment.client_id}/rsyncers"
            ).json()
            prompt += f"Copied {sum(r['files_counted'] for r in rsync_instances)} / {sum(r['files_transferred'] for r in rsync_instances)}"
            self.install_screen(
                WaitingScreen(prompt, sum(r["files_counted"] for r in rsync_instances)),
                "waiting",
            )
            self.push_screen("waiting")

    async def action_quit(self) -> None:
        log.info("quitting app")

        if self.rsync_processes:
            for rp in self.rsync_processes.values():
                rp.stop()
        if self.analysers:
            for a in self.analysers.values():
                a.stop()
        if self._multigrid_watcher:
            self._multigrid_watcher.stop()
        self.exit()
        exit()

    async def action_remove_session(self) -> None:
        requests.delete(
            f"{self._environment.url.geturl()}/clients/{self._environment.client_id}/session"
        )
        if self.rsync_processes:
            for rp in self.rsync_processes.values():
                rp.stop()
        if self.analysers:
            for a in self.analysers.values():
                a.stop()
        if self._multigrid_watcher:
            self._multigrid_watcher.stop()
        self.exit()
        exit()

    def clean_up_quit(self) -> None:
        requests.delete(
            f"{self._environment.url.geturl()}/clients/{self._environment.client_id}/session"
        )
        self.exit()
        exit()

    async def action_clear(self) -> None:
        machine_config = get_machine_config(
            str(self._environment.url.geturl()), demo=self._environment.demo
        )
        if self.rsync_processes and machine_config.get("allow_removal"):
            sources = "\n".join(str(k) for k in self.rsync_processes.keys())
            prompt = f"Remove files from the following: {sources}"
            self.install_screen(
                ConfirmScreen(
                    prompt,
                    pressed_callback=self._remove_data,
                    button_names={"launch": "Yes", "quit": "No"},
                ),
                "clear-confirm",
            )
            self.push_screen("clear-confirm")

    def _remove_data(self, listener: Callable[..., Awaitable[None] | None], **kwargs):
        new_rsyncers = []
        if self.rsync_processes or self._environment.demo:
            for k, rp in self.rsync_processes.items():
                rp.stop()
                if self.analysers.get(k):
                    self.analysers[k].stop()
                removal_rp = RSyncer.from_rsyncer(rp, remove_files=True, notify=False)
                removal_rp.subscribe(listener)
                new_rsyncers.append(removal_rp)
        log.info(
            f"Starting to remove data files {self._environment.demo}, {len(self.rsync_processes)}"
        )
        for removal_rp in new_rsyncers:
            removal_rp.start()
            for f in k.absolute().glob("**/*"):
                removal_rp.queue.put(f)
            removal_rp.stop()
            log.info(f"rsyncer {rp} rerun with removal")
        requests.post(
            f"{self._environment.url.geturl()}/clients/{self._environment.client_id}/successful_processing"
        )
        requests.delete(
            f"{self._environment.url.geturl()}/clients/{self._environment.client_id}/session"
        )
        self.exit()
        exit()

    async def action_process(self) -> None:
        self.processing_btn.disabled = False

    def _set_default_acquisition_directories(self, default_dir: Path):
        set_default_acquisition_output(
            default_dir, self._machine_config["software_settings_output_directories"]
        )
