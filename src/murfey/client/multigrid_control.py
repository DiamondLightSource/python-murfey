import json
import logging
import subprocess
import threading
from dataclasses import dataclass, field
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

import requests

import murfey.client.websocket
from murfey.client.analyser import Analyser
from murfey.client.contexts.spa import SPAModularContext
from murfey.client.contexts.tomo import TomographyContext
from murfey.client.instance_environment import MurfeyInstanceEnvironment
from murfey.client.rsync import RSyncer, RSyncerUpdate, TransferResult
from murfey.client.tui.screens import determine_default_destination
from murfey.client.watchdir import DirWatcher
from murfey.util import capture_post, get_machine_config_client, posix_path

log = logging.getLogger("murfey.client.mutligrid_control")


@dataclass
class MultigridController:
    sources: list[Path]
    visit: str
    instrument_name: str
    session_id: int
    murfey_url: str = "http://localhost:8000"
    rsync_url: str = ""
    rsync_module: str = "data"
    demo: bool = False
    processing_enabled: bool = True
    do_transfer: bool = True
    dummy_dc: bool = False
    force_mdoc_metadata: bool = True
    rsync_restarts: List[str] = field(default_factory=lambda: [])
    rsync_processes: Dict[Path, RSyncer] = field(default_factory=lambda: {})
    analysers: Dict[Path, Analyser] = field(default_factory=lambda: {})
    data_collection_parameters: dict = field(default_factory=lambda: {})
    token: str = ""
    _machine_config: dict = field(default_factory=lambda: {})

    def __post_init__(self):
        if self.token:
            requests.get = partial(
                requests.get, headers={"Authorization": f"Bearer {self.token}"}
            )
            requests.post = partial(
                requests.post, headers={"Authorization": f"Bearer {self.token}"}
            )
            requests.delete = partial(
                requests.delete, headers={"Authorization": f"Bearer {self.token}"}
            )
        machine_data = requests.get(
            f"{self.murfey_url}/instruments/{self.instrument_name}/machine"
        ).json()
        self.rsync_url = machine_data.get("rsync_url", "")
        self.rsync_module = machine_data.get("rsync_module", "data")
        self._environment = MurfeyInstanceEnvironment(
            url=urlparse(self.murfey_url, allow_fragments=False),
            client_id=0,
            murfey_session=self.session_id,
            software_versions=machine_data.get("software_versions", {}),
            default_destination=f"{datetime.now().year}",
            demo=self.demo,
            visit=self.visit,
            data_collection_parameters=self.data_collection_parameters,
            instrument_name=self.instrument_name,
            # processing_only_mode=server_routing_prefix_found,
        )
        self._machine_config = get_machine_config_client(
            str(self._environment.url.geturl()),
            instrument_name=self._environment.instrument_name,
            demo=self._environment.demo,
        )
        self._data_suffixes = (".mrc", ".tiff", ".tif", ".eer")
        self._data_substrings = [
            s
            for val in self._machine_config["data_required_substrings"].values()
            for ds in val.values()
            for s in ds
        ]
        self._data_collection_form_complete = False
        self._register_dc: bool | None = None
        self.rsync_processes = self.rsync_processes or {}
        self.analysers = self.analysers or {}

        self.ws = murfey.client.websocket.WSApp(
            server=self.murfey_url,
            register_client=False,
        )

    def _start_rsyncer_multigrid(
        self,
        source: Path,
        extra_directory: str = "",
        include_mid_path: bool = True,
        use_suggested_path: bool = True,
        destination_overrides: Optional[Dict[Path, str]] = None,
        remove_files: bool = False,
        analyse: bool = True,
        tag: str = "",
        limited: bool = False,
    ):
        log.info(f"starting multigrid rsyncer: {source}")
        destination_overrides = destination_overrides or {}
        machine_data = requests.get(
            f"{self._environment.url.geturl()}/instruments/{self.instrument_name}/machine"
        ).json()
        if destination_overrides.get(source):
            destination = (
                destination_overrides[source]
                if str(source) in self.rsync_restarts
                else destination_overrides[source] + f"/{extra_directory}"
            )
        else:
            for k, v in destination_overrides.items():
                if Path(v).name in source.parts:
                    destination = str(k / extra_directory)
                    break
            else:
                self._environment.default_destinations[source] = (
                    f"{datetime.now().year}"
                )
                destination = determine_default_destination(
                    self._environment.visit,
                    source,
                    self._environment.default_destinations[source],
                    self._environment,
                    self.analysers or {},
                    touch=True,
                    extra_directory=extra_directory,
                    include_mid_path=include_mid_path,
                    use_suggested_path=use_suggested_path,
                )
        self._environment.sources.append(source)
        self._start_rsyncer(
            source,
            destination,
            force_metadata=self.processing_enabled,
            analyse=analyse,
            remove_files=remove_files,
            tag=tag,
            limited=limited,
            transfer=machine_data.get("data_transfer_enabled", True),
            restarted=str(source) in self.rsync_restarts,
        )
        self.ws.send(json.dumps({"message": "refresh"}))

    def _rsyncer_stopped(self, source: Path, explicit_stop: bool = False):
        if explicit_stop:
            remove_url = (
                f"{self.murfey_url}/sessions/{self.session_id}/rsyncer/{str(source)}"
            )
            requests.delete(remove_url)
        else:
            stop_url = f"{self.murfey_url}/sessions/{self.session_id}/rsyncer_stopped"
            capture_post(stop_url, json={"source": str(source)})

    def _finalise_rsyncer(self, source: Path):
        finalise_thread = threading.Thread(
            name=f"Controller finaliser thread ({source})",
            target=self.rsync_processes[source].finalise,
            kwargs={"thread": False},
            daemon=True,
        )
        finalise_thread.start()

    def _restart_rsyncer(self, source: Path):
        self.rsync_processes[source].restart()
        restarted_url = f"{self.murfey_url}/sessions/{self.session_id}/rsyncer_started"
        capture_post(restarted_url, json={"source": str(source)})

    def _request_watcher_stop(self, source: Path):
        self._environment.watchers[source]._stopping = True
        self._environment.watchers[source]._halt_thread = True

    def _start_rsyncer(
        self,
        source: Path,
        destination: str,
        visit_path: str = "",
        force_metadata: bool = False,
        analyse: bool = True,
        remove_files: bool = False,
        tag: str = "",
        limited: bool = False,
        transfer: bool = True,
        restarted: bool = False,
    ):
        log.info(f"starting rsyncer: {source}")
        if transfer:
            # Always make sure the destination directory exists
            make_directory_url = (
                f"{self.murfey_url}/sessions/{self.session_id}/make_rsyncer_destination"
            )
            capture_post(make_directory_url, json={"destination": destination})
        if self._environment:
            self._environment.default_destinations[source] = destination
            if self._environment.gain_ref and visit_path:
                # Set up rsync command
                rsync_cmd = [
                    "rsync",
                    f"{posix_path(self._environment.gain_ref)!r}",  # '!r' will print strings in ''
                    f"{self._environment.url.hostname}::{self.rsync_module}/{visit_path}/processing",
                ]
                # Wrap in bash shell
                cmd = [
                    "bash",
                    "-c",
                    " ".join(rsync_cmd),
                ]
                # Run rsync subprocess
                gain_rsync = subprocess.run(cmd)
                if gain_rsync.returncode:
                    log.warning(
                        f"Gain reference file {posix_path(self._environment.gain_ref)!r} was not successfully transferred to {visit_path}/processing"
                    )
        if transfer:
            self.rsync_processes[source] = RSyncer(
                source,
                basepath_remote=Path(destination),
                rsync_module=self.rsync_module,
                server_url=(
                    urlparse(self.rsync_url)
                    if self.rsync_url
                    else self._environment.url
                ),
                stop_callback=self._rsyncer_stopped,
                do_transfer=self.do_transfer,
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
            if restarted:
                restarted_url = (
                    f"{self.murfey_url}/sessions/{self.session_id}/rsyncer_started"
                )
                capture_post(restarted_url, json={"source": str(source)})
            else:
                url = f"{str(self._environment.url.geturl())}/sessions/{str(self._environment.murfey_session)}/rsyncer"
                rsyncer_data = {
                    "source": str(source),
                    "destination": destination,
                    "session_id": self.session_id,
                    "transferring": self.do_transfer or self._environment.demo,
                    "tag": tag,
                }
                requests.post(url, json=rsyncer_data)
        self._environment.watchers[source] = DirWatcher(source, settling_time=30)

        if not self.analysers.get(source) and analyse:
            log.info(f"Starting analyser for {source}")
            self.analysers[source] = Analyser(
                source,
                environment=self._environment if not self.dummy_dc else None,
                force_mdoc_metadata=self.force_mdoc_metadata,
                limited=limited,
            )
            if force_metadata:
                self.analysers[source].subscribe(
                    partial(self._start_dc, from_form=True)
                )
            else:
                self.analysers[source].subscribe(self._data_collection_form)
            self.analysers[source].start()
            if transfer:
                self.rsync_processes[source].subscribe(self.analysers[source].enqueue)

        if transfer:
            self.rsync_processes[source].start()

        if self._environment:
            if self._environment.watchers.get(source):
                if transfer:
                    self._environment.watchers[source].subscribe(
                        self.rsync_processes[source].enqueue
                    )
                else:
                    # the watcher and rsyncer don't notify with the same object so conversion required here
                    def _rsync_update_converter(p: Path) -> None:
                        self.analysers[source].enqueue(
                            RSyncerUpdate(
                                file_path=p,
                                file_size=0,
                                outcome=TransferResult.SUCCESS,
                                transfer_total=0,
                                queue_size=0,
                                base_path=source,
                            )
                        )
                        return None

                    self._environment.watchers[source].subscribe(
                        _rsync_update_converter
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
            self._data_collection_form_complete = True
        elif self._register_dc is None:
            self._data_collection_form_complete = True

    def _start_dc(self, json, from_form: bool = False):
        if self.dummy_dc:
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
            if from_form:
                requests.post(
                    f"{self._environment.url.geturl()}/clients/{self._environment.client_id}/tomography_processing_parameters",
                    json=json,
                )

            source = Path(json["source"])
            context.register_tomography_data_collections(
                file_extension=json["file_extension"],
                image_directory=str(self._environment.default_destinations[source]),
                environment=self._environment,
            )

            log.info("Registering tomography processing parameters")
            if self._environment.data_collection_parameters.get("num_eer_frames"):
                eer_response = requests.post(
                    f"{str(self._environment.url.geturl())}/visits/{self._environment.visit}/{self._environment.murfey_session}/eer_fractionation_file",
                    json={
                        "num_frames": self._environment.data_collection_parameters[
                            "num_eer_frames"
                        ],
                        "fractionation": self._environment.data_collection_parameters[
                            "eer_fractionation"
                        ],
                        "dose_per_frame": self._environment.data_collection_parameters[
                            "dose_per_frame"
                        ],
                        "fractionation_file_name": "eer_fractionation_tomo.txt",
                    },
                )
                eer_fractionation_file = eer_response.json()["eer_fractionation_file"]
                json.update({"eer_fractionation_file": eer_fractionation_file})
            requests.post(
                f"{self._environment.url.geturl()}/sessions/{self._environment.murfey_session}/tomography_preprocessing_parameters",
                json=json,
            )
            capture_post(
                f"{self._environment.url.geturl()}/visits/{self._environment.visit}/{self._environment.murfey_session}/flush_tomography_processing",
                json={"rsync_source": str(source)},
            )
            log.info("Tomography processing flushed")

        elif isinstance(context, SPAModularContext):
            url = f"{str(self._environment.url.geturl())}/visits/{str(self._environment.visit)}/{self.session_id}/register_data_collection_group"
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
                    f"{str(self._environment.url.geturl())}/visits/{str(self._environment.visit)}/{self.session_id}/start_data_collection",
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
                        f"{str(self._environment.url.geturl())}/visits/{str(self._environment.visit)}/{self.session_id}/register_processing_job",
                        json={
                            "tag": str(source),
                            "source": str(source),
                            "recipe": recipe,
                        },
                    )
                log.info(f"Posting SPA processing parameters: {json}")
                response = capture_post(
                    f"{self._environment.url.geturl()}/sessions/{self.session_id}/spa_processing_parameters",
                    json={
                        **{k: None if v == "None" else v for k, v in json.items()},
                        "tag": str(source),
                    },
                )
                if response and not str(response.status_code).startswith("2"):
                    log.warning(f"{response.reason}")
                capture_post(
                    f"{self._environment.url.geturl()}/visits/{self._environment.visit}/{self.session_id}/flush_spa_processing",
                    json={"tag": str(source)},
                )

    def _increment_file_count(
        self, observed_files: List[Path], source: str, destination: str
    ):
        url = f"{str(self._environment.url.geturl())}/visits/{str(self._environment.visit)}/increment_rsync_file_count"
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
            "session_id": self.session_id,
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
            url = f"{str(self._environment.url.geturl())}/visits/{str(self._environment.visit)}/increment_rsync_transferred_files_prometheus"
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
                "session_id": self.session_id,
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
        url = f"{str(self._environment.url.geturl())}/visits/{str(self._environment.visit)}/increment_rsync_transferred_files"
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
            "session_id": self.session_id,
            "increment_count": len(checked_updates),
            "bytes": sum(f.file_size for f in checked_updates),
            "increment_data_count": len(data_files),
            "data_bytes": sum(f.file_size for f in data_files),
        }
        requests.post(url, json=data)
