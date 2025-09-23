# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

from typing import Any, Dict, List, Optional

from .processor import BaseProcessor, ProcessorResult
from .registry import ProcessorRegistry


class ProcessorChain:
    """
    Chain of processors to process events.
    """

    def __init__(self, processors: List[BaseProcessor]) -> None:
        self.processors = processors

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:

        if context is None:
            context = {}

        current_result = ProcessorResult(event)

        for processor in self.processors:
            if current_result.is_empty:
                break

            # Process each event from the current result
            processed_events = []
            for single_event in current_result.events:
                result = processor.process(single_event, context)
                if not result.is_empty:
                    processed_events.extend(result.events)

            # If no events were processed successfully, stop and return current result
            if not processed_events:
                break

            current_result = ProcessorResult(processed_events)

        return current_result


class ProcessorFactory:
    """
    Factory for creating processor instances and processor chains.
    """

    @staticmethod
    def create(processor_type: str, **kwargs: Any) -> BaseProcessor:

        processor_class = ProcessorRegistry.get(processor_type)
        processor = processor_class()
        processor.configure(kwargs)
        return processor

    @staticmethod
    def create_chain(processor_configs: List[Dict[str, Any]]) -> ProcessorChain:
        """
        Create a processor chain from a list of processor configurations.

        Args:
            processor_configs: List of processor configurations, each containing
                             a 'type' field and optional configuration fields

        Returns:
            A processor chain

        Raises:
            ValueError: If any processor type is unknown or configuration is invalid

        Example:
            configs = [
                {"type": "filter", "filter_field": "level", "filter_values": ["ERROR"]},
                {"type": "add_field", "fields": {"processed": True}}
            ]
            chain = ProcessorFactory.create_chain(configs)
        """
        processors = []

        for config in processor_configs:
            if "type" not in config:
                raise ValueError("Processor configuration must include 'type' field")

            processor_type = config.pop("type")
            processor = ProcessorFactory.create(processor_type, **config)
            processors.append(processor)

        return ProcessorChain(processors)
