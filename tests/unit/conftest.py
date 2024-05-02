import os
from argparse import Namespace
from copy import deepcopy
from multiprocessing import get_context
from typing import Any, Dict

import pytest
from dbt.config import PartialProject, Profile, Project, RuntimeConfig
from dbt.config.renderer import DbtProjectYamlRenderer, ProfileRenderer
from dbt.flags import set_from_args

from dbt.adapters.synapse import SynapseAdapter

set_from_args(Namespace(), None)


@pytest.fixture
def adapter() -> SynapseAdapter:
    project_cfg = {
        "name": "X",
        "version": "0.1",
        "profile": "test",
        "project-root": "/tmp/dbt/does-not-exist",
        "config-version": 2,
    }
    profile_cfg = {
        "outputs": {
            "test": {
                "type": "synapse",
                "driver": "baby",
                "host": "thishostshouldnotexist",
                "port": 1433,
                "database": "synapse",
                "user": "root",
                "pass": "password",
                "schema": "default",
            }
        },
        "target": "test",
    }
    config = config_from_parts_or_dicts(project_cfg, profile_cfg)
    mp_context = get_context("spawn")
    return SynapseAdapter(config, mp_context)


def config_from_parts_or_dicts(project: Dict[str, Any], profile: Dict[str, Any]) -> RuntimeConfig:
    profile_name = project.get("profile")
    profile = _profile_from_dict(deepcopy(profile), profile_name)
    project = _project_from_dict(deepcopy(project), profile)
    args = Namespace(
        which="blah",
        single_threaded=False,
        profile_dir="/dev/null",
    )
    return RuntimeConfig.from_parts(project, profile, args)


def _profile_from_dict(profile: Dict[str, Any], profile_name: str) -> Profile:
    renderer = ProfileRenderer()
    return Profile.from_raw_profile_info(profile, profile_name, renderer)


def _project_from_dict(project: Dict[str, Any], profile: Profile) -> Project:
    project_root = project.pop("project-root", os.getcwd())
    partial = PartialProject.from_dicts(project_root, project, {}, {})
    renderer = DbtProjectYamlRenderer(profile=profile)
    return partial.render(renderer)
