# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Dict, Any, Optional

from processors.processor import BaseProcessor, ProcessorResult
from processors.registry import register_processor


@register_processor("passthrough")
class PassThroughProcessor(BaseProcessor):
    """
    Passthrough processor for initial testing
    """

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
        return ProcessorResult(event)
