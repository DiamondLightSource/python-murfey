from __future__ import annotations

import asyncio

# import contextlib
import logging
from datetime import datetime
from functools import partial
from pathlib import Path
from queue import Queue
from typing import Dict, List, OrderedDict, TypeVar
from urllib.parse import urlparse

import procrunner
import requests
from textual.app import App
from textual.reactive import reactive
from textual.widgets import Button, Input

from murfey.client.analyser import Analyser
from murfey.client.context import SPAContext, TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncer, RSyncerUpdate, TransferResult
from murfey.client.tui.screens import (
    InputResponse,
    MainScreen,
    ProcessingForm,
    VisitSelection,
    determine_default_destination,
)
from murfey.client.tui.status_bar import StatusBar
from murfey.client.watchdir import DirWatcher
from murfey.client.watchdir_multigrid import MultigridDirWatcher
from murfey.util import _get_visit_list

log = logging.getLogger("murfey.tui.app")

ReactiveType = TypeVar("ReactiveType")


class MurfeyTUI(App):
    CSS_PATH = "controller.css"
    processing_btn: Button
    processing_form: ProcessingForm
    hover: List[str]
    visits: List[str]
    rsync_processes: Dict[Path, RSyncer] = {}
    analysers: Dict[Path, Analyser] = {}
    _form_values: dict = reactive({})

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
        self._multigrid_watcher: MultigridDirWatcher | None = None
        self._force_mdoc_metadata = force_mdoc_metadata
        self._strict = strict
        self.install_screen(MainScreen(), "main")

    @property
    def role(self) -> str:
        if self.analyser:
            return self.analyser._role
        return ""

    def _launch_multigrid_watcher(self, source: Path):
        log.info(f"Launching multigrid watcher for source {source}")
        self._multigrid_watcher = MultigridDirWatcher(source)
        self._multigrid_watcher.subscribe(self._start_rsyncer_multigrid)
        self._multigrid_watcher.start()

    def _start_rsyncer_multigrid(
        self,
        source: Path,
        extra_directory: str = "",
        include_mid_path: bool = True,
        use_suggested_path: bool = True,
    ):
        machine_data = requests.get(f"{self._environment.url.geturl()}/machine/").json()
        self._environment.default_destinations[
            source
        ] = f"{machine_data.get('rsync_module') or 'data'}/{datetime.now().year}"
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
        self._start_rsyncer(source, destination, force_metadata=True)

    def _start_rsyncer(
        self,
        source: Path,
        destination: str,
        visit_path: str = "",
        force_metadata: bool = False,
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
            local=self._environment.demo,
            status_bar=self._statusbar,
            do_transfer=self._do_transfer,
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
        self._environment.watchers[source] = DirWatcher(source, settling_time=1)

        if not self.analysers.get(source):
            log.info(f"Starting analyser for {source}")
            self.analysers[source] = Analyser(
                source,
                environment=self._environment if not self._dummy_dc else None,
                force_mdoc_metadata=self._force_mdoc_metadata,
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
                self._environment.watchers[source].start()

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
            json = {k: str(v) for k, v in json.items()}
        self._environment.data_collection_parameters = {
            k: None if v == "None" else v for k, v in json.items()
        }
        source = Path(json["source"])
        context = self.analysers[source]._context
        if isinstance(context, TomographyContext):
            source = Path(json["source"])
            self._environment.listeners["data_collection_group_ids"] = {
                context._flush_data_collections
            }
            self._environment.listeners["data_collection_ids"] = {
                context._flush_processing_job
            }
            self._environment.listeners["autoproc_program_ids"] = {
                context._flush_preprocess
            }
            self._environment.listeners["motion_corrected_movies"] = {
                context._check_for_alignment
            }
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/register_data_collection_group"
            dcg_data = {
                "experiment_type": "tomo",
                "experiment_type_id": 36,
                "tag": str(source),
            }
            requests.post(url, json=dcg_data)
        elif isinstance(context, SPAContext):
            source = Path(json["source"])
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/start_data_collection"
            json = {
                "tag": str(source.resolve()),
                "image_directory": str(
                    Path(self._environment.default_destinations[source]).resolve()
                ),
                **json,
            }
            self._environment.listeners["data_collection_group_ids"] = {
                partial(
                    context._register_data_collection,
                    url=url,
                    data=json,
                )
            }
            self._environment.listeners["data_collection_ids"] = {
                partial(
                    context._register_processing_job,
                    parameters=json,
                )
            }
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/spa_processing"
            self._environment.listeners["processing_job_ids"] = {
                partial(context._launch_spa_pipeline, url=url)
            }
            url = f"{str(self._url.geturl())}/visits/{str(self._visit)}/register_data_collection_group"
            dcg_data = {
                "experiment_type": "single particle",
                "experiment_type_id": 37,
                "tag": str(source),
            }
            requests.post(url, json=dcg_data)

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

    def on_input_submitted(self, event: Input.Submitted):
        event.input.has_focus = False
        self.screen.focused = None

    def on_button_pressed(self, event: Button.Pressed):
        if event.button._id == "processing-btn":
            self._install_processing_form()
            self.push_screen("processing-form")
        elif event.button._id == "new-visit-btn":
            self.reset()
            if self.rsync_processes:
                for rp in self.rsync_processes.values():
                    rp.stop()
            if self.analysers:
                for a in self.analysers.values():
                    a.stop()
            self.rsync_processes = {}
            self.analysers = {}
            self.push_screen("visit-select-screen")

    async def on_mount(self) -> None:
        self.install_screen(VisitSelection(self.visits), "visit-select-screen")
        self.push_screen("visit-select-screen")

    def on_log_book_log(self, message):
        self.log_book.write(message.renderable)

    def reset(self):
        self._environment.clear()
        if self.rsync_processes:
            for rp in self.rsync_processes.values():
                rp.stop()
            self.rsync_processes = {}
        if self.analysers:
            for a in self.analysers.values():
                a.stop()
            self.analysers = {}
        self.visits = [v.name for v in _get_visit_list(self._environment.url)]
        self._default_destinations = self._environment.default_destinations
        self._data_collection_form_complete = False
        self._form_values = {}
        self.uninstall_screen("visit-select-screen")
        self.uninstall_screen("launcher")
        self.uninstall_screen("destination-select-screen")
        self.uninstall_screen("processing-form")
        self.uninstall_screen("directory-select")
        self.pop_screen()
        self.uninstall_screen("main")
        self.install_screen(MainScreen(), "main")
        self.push_screen("main")
        self.install_screen(VisitSelection(self.visits), "visit-select-screen")
        self.push_screen("visit-select-screen")

    async def action_quit(self) -> None:
        log.info("quitting app")

        if self.rsync_processes:
            for rp in self.rsync_processes.values():
                rp.stop()
        if self.analysers:
            for a in self.analysers.values():
                a.stop()
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
