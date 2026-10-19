"""Public exports; implementation modules are separated by responsibility."""

from deckr.substrates._supervised_substrate import (
    SupervisedNatsSubstrate as SupervisedNatsSubstrate,
)
from deckr.substrates.nats_binary import (
    _DEFAULT_HOST as _DEFAULT_HOST,
    _DEFAULT_MINIMUM_VERSION as _DEFAULT_MINIMUM_VERSION,
    _DEFAULT_PORT as _DEFAULT_PORT,
    _SIGKILL as _SIGKILL,
    _VERSION_RE as _VERSION_RE,
    NatsServerBinaryResolutionError as NatsServerBinaryResolutionError,
    NatsServerBinaryResolver as NatsServerBinaryResolver,
    NatsServerProcessExited as NatsServerProcessExited,
    NatsServerStartupError as NatsServerStartupError,
    NatsServerSupervisorError as NatsServerSupervisorError,
    NatsServerVersionError as NatsServerVersionError,
    ResolvedNatsServerBinary as ResolvedNatsServerBinary,
    _assert_minimum_version as _assert_minimum_version,
    _bundled_nats_server_path as _bundled_nats_server_path,
    _read_nats_server_version as _read_nats_server_version,
    _version_key as _version_key,
    logger as logger,
)
from deckr.substrates.nats_server import (
    NatsServerHandle as NatsServerHandle,
    NatsServerSupervisor as NatsServerSupervisor,
    _chmod_private_directory as _chmod_private_directory,
    _config_text as _config_text,
    _failure_message as _failure_message,
    _nats_url_from_ports_file as _nats_url_from_ports_file,
    _remove_stale_ports_files as _remove_stale_ports_files,
    _signal_process_group as _signal_process_group,
    _verify_jetstream as _verify_jetstream,
    _write_private_text as _write_private_text,
)

__all__ = [
    "NatsServerBinaryResolutionError",
    "NatsServerBinaryResolver",
    "NatsServerHandle",
    "NatsServerProcessExited",
    "NatsServerStartupError",
    "NatsServerSupervisor",
    "NatsServerSupervisorError",
    "NatsServerVersionError",
    "ResolvedNatsServerBinary",
    "SupervisedNatsSubstrate",
]
