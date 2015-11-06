########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

from cloudify import ctx
from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError


@operation
def assert_agent_validation_succeeded(node_instances_id, **_):
    for node_instance_id in node_instances_id:
        node_instance = ctx.deployment.get_node_instance(node_instance_id)
        if 'agent_status' not in node_instance.runtime_properties:
            raise NonRecoverableError(
                ('No validation results for node instance '
                 '{0}').format(node_instance_id))
        agent_status = node_instance.runtime_properties['agent_status']
        if not agent_status.get('agent_alive_crossbroker'):
            raise NonRecoverableError(
                'Could not connect to agent on {0}'.format(node_instance_id))
