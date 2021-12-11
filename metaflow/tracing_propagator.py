# Copyright The OpenTelemetry Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import typing
import logging

from opentelemetry.context import Context

from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.context.context import Context
from opentelemetry.propagators.textmap import (
    DefaultGetter,
    DefaultSetter,
    Getter,
    Setter,
    TextMapPropagator,
    CarrierT,
    CarrierValT,
)


class EnvPropagator(TextMapPropagator):
    def __init__(self, formatter):
        if formatter is None:
            self.formatter = TraceContextTextMapPropagator()
        else:
            self.formatter = formatter

    # delegating to extract function implementation of the formatter
    def extract(
        self,
        carrier: CarrierT,
        context: typing.Optional[Context] = None,
        getter: Getter = DefaultGetter(),
    ) -> Context:
        return self.formatter.extract(carrier=carrier, context=context, getter=getter)

    # delegating to inject function implementation of the formatter
    def inject(
        self,
        carrier: CarrierT,
        context: typing.Optional[Context] = None,
        setter: Setter = DefaultSetter(),
    ) -> None:
        self.formatter.inject(carrier=carrier, context=context, setter=setter)

    # function for the user to inject trace details or baggage
    def inject_to_carrier(self, context: typing.Optional[Context] = None):
        env_dict = os.environ.copy()
        self.inject(carrier=env_dict, context=context, setter=DefaultSetter())
        return env_dict

    # function for the user to extract trace context or baggage
    def extract_context(self) -> Context:
        if self.formatter is None:
            self.formatter = TraceContextTextMapPropagator()

        return self.extract(carrier=os.environ, getter=DefaultGetter())

    @property
    def fields(self) -> typing.Set[str]:
        # Returns a set with the fields set in `inject`.
        return self.formatter.fields
