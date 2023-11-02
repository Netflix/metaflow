# Copyright 2018 The Kubeflow Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import tempfile

from deprecated.sphinx import deprecated

from ..compiler import build_docker_image


@deprecated(
    version='0.1.32',
    reason='The %%docker magic is deprecated. Use `kfp.containers.build_image_from_working_dir` instead.'
)
def docker(line, cell):
    """cell magic for %%docker."""

    if len(line.split()) < 2:
        raise ValueError(
            "usage: %%docker [gcr.io/project/image:tag] [gs://staging-bucket] [600] [kubeflow]\n\
                      arg1(required): target image tag\n\
                      arg2(required): staging gcs bucket\n\
                      arg3(optional): timeout in seconds, default(600)\n\
                      arg4(optional): namespace, default(kubeflow)")
    if not cell.strip():
        raise ValueError("Please fill in a dockerfile content in the cell.")

    fields = line.split()

    with tempfile.NamedTemporaryFile(mode='wt', delete=False) as f:
        f.write(cell)

    build_docker_image(
        fields[1],
        fields[0],
        f.name,
        timeout=int(fields[2]) if len(fields) >= 3 else 600,
        namespace=fields[3] if len(fields) >= 4 else 'kubeflow')
    os.remove(f.name)


try:
    import IPython
    docker = IPython.core.magic.register_cell_magic(docker)
except ImportError:
    pass
except NameError:
    pass
