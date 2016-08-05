########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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

import threading
import testtools
from Queue import Queue


from cloudify.state import ctx, current_ctx, NotInContext

from cloudify.mocks import MockCloudifyContext


class TestCurrentContextAndCtxLocalProxy(testtools.TestCase):

    def test_basic(self):
        self.assertRaises(NotInContext, current_ctx.get_ctx)
        self.assertRaises(NotInContext, lambda: ctx.instance.id)
        value = MockCloudifyContext(node_id='1')
        current_ctx.set(value)
        self.assertEqual(value, current_ctx.get_ctx())
        self.assertEqual(value.instance.id, ctx.instance.id)
        current_ctx.clear()
        self.assertRaises(NotInContext, current_ctx.get_ctx)
        self.assertRaises(NotInContext, lambda: ctx.instance.id)

    def test_threads(self):
        num_iterations = 1000
        num_threads = 10
        for _ in range(num_iterations):
            queues = [Queue() for _ in range(num_threads)]

            def run(queue, value):
                try:
                    self.assertRaises(NotInContext, current_ctx.get_ctx)
                    self.assertRaises(NotInContext, lambda: ctx.instance.id)
                    current_ctx.set(value)
                    self.assertEqual(value, current_ctx.get_ctx())
                    self.assertEqual(value.instance.id, ctx.instance.id)
                    current_ctx.clear()
                    self.assertRaises(NotInContext, current_ctx.get_ctx)
                    self.assertRaises(NotInContext, lambda: ctx.instance.id)
                except Exception as e:
                    queue.put(e)
                else:
                    queue.put('ok')

            threads = []
            for index, queue in enumerate(queues):
                value = MockCloudifyContext(node_id=str(index))
                threads.append(threading.Thread(target=run,
                                                args=(queue, value)))

            for thread in threads:
                thread.start()

            for queue in queues:
                self.assertEqual('ok', queue.get())

    def test_push_sets_context(self):
        """Inside a `with ctx.push()` block, the ctx is actually set."""
        new_ctx, new_params = object(), {'value': object()}
        with current_ctx.push(new_ctx, new_params):
            self.assertIs(new_ctx, current_ctx.get_ctx())
            self.assertEqual(new_params, current_ctx.get_parameters())

    def test_push_reverts(self):
        """With no previous context, exiting a push block clears the ctx."""
        new_ctx, new_params = object(), {'value': object()}

        with current_ctx.push(new_ctx, new_params):
            pass

        self.assertRaises(NotInContext, current_ctx.get_ctx)
        self.assertRaises(NotInContext, current_ctx.get_parameters)

    def test_push_reverts_preexisting(self):
        """Exiting a push block, sets the previous context."""
        ctx, params = object(), {'value': object()}
        new_ctx, new_params = object(), {'value': object()}
        current_ctx.set(ctx, params)

        with current_ctx.push(new_ctx, new_params):
            pass

        self.assertIs(ctx, current_ctx.get_ctx())
        self.assertEqual(params, current_ctx.get_parameters())
