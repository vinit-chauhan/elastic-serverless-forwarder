# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

"""
Tests for the Processor System used in S3 Binary Processing Flow

This module tests the processor system components including:
- Base processor functionality and configuration
- Processor chains and their execution
- Processor factory and registration
- Integration with IPFIX and other binary processing
"""

from unittest import TestCase
from unittest.mock import patch
from typing import Any, Dict, Optional

import pytest

from processors.factory import ProcessorChain, ProcessorFactory
from processors.passthrough import PassThroughProcessor
from processors.ipfix_ecs import ECSProcessor
from processors.processor import BaseProcessor, ProcessorResult
from processors.utils import process_event


class MockProcessor(BaseProcessor):
    """Mock processor for testing"""

    def __init__(self):
        super().__init__()
        self.processed_events = []

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
        # Add a test field to track processing
        processed_event = event.copy()
        processed_event["processed_by_mock"] = True
        processed_event["processor_config"] = self._config

        self.processed_events.append(processed_event)
        return ProcessorResult(processed_event)


class MockFilterProcessor(BaseProcessor):
    """Mock processor that filters events based on condition"""

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
        filter_field = self._config.get("filter_field")
        filter_value = self._config.get("filter_value")

        if filter_field and filter_value:
            if event.get(filter_field) == filter_value:
                return ProcessorResult()  # Empty result to stop chain

        return ProcessorResult(event)


class MockEmptyProcessor(BaseProcessor):
    """Mock processor that always returns empty result"""

    def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
        return ProcessorResult()  # Always empty


class TestProcessorSystem(TestCase):
    """Test the processor system components"""

    def setUp(self):
        """Set up test environment"""
        self.sample_event = {
            "@timestamp": "2025-01-26T10:30:00Z",
            "fields": {
                "message": '{"timestamp": "2025-01-26T10:30:00Z", "level": "INFO", "message": "Test message"}',
                "log": {"offset": 0, "file": {"path": "s3://bucket/key"}},
                "aws": {
                    "s3": {
                        "bucket": {"name": "test-bucket", "arn": "arn:aws:s3:::test-bucket"},
                        "object": {"key": "test-key"},
                    }
                },
            },
        }

        # Sample IPFIX event for testing ECS processor (in expected format)
        self.sample_ipfix_event = {
            "@timestamp": "2025-01-26T10:30:00Z",
            "fields": {
                "message": {
                    "sourceIPv4Address": "192.168.1.100",
                    "destinationIPv4Address": "10.0.0.50",
                    "sourceTransportPort": 443,
                    "destinationTransportPort": 80,
                    "protocolIdentifier": 6,
                    "octetDeltaCount": 1024,
                    "@timestamp": "2025-01-26T10:30:00Z",
                    "header": {"version": 10, "export_time": 1719403200, "sequence_number": 1},
                },
                "aws": {
                    "s3": {
                        "bucket": {"name": "ipfix-bucket", "arn": "arn:aws:s3:::ipfix-bucket"},
                        "object": {"key": "flow-data.ipfix"},
                    }
                },
            },
        }

    @pytest.mark.unit
    def test_processor_result_creation(self):
        """Test ProcessorResult creation and methods"""
        # Test empty result
        empty_result = ProcessorResult()
        self.assertTrue(empty_result.is_empty)
        self.assertEqual(len(empty_result), 0)
        self.assertEqual(empty_result.to_dict(), {})

        # Test single event result
        single_result = ProcessorResult(self.sample_event)
        self.assertFalse(single_result.is_empty)
        self.assertEqual(len(single_result), 1)
        self.assertEqual(single_result.to_dict(), self.sample_event)

        # Test multiple events result
        multiple_events = [self.sample_event, self.sample_event.copy()]
        multiple_result = ProcessorResult(multiple_events)
        self.assertFalse(multiple_result.is_empty)
        self.assertEqual(len(multiple_result), 2)
        result_dict = multiple_result.to_dict()
        self.assertIn("0", result_dict)
        self.assertIn("1", result_dict)

    @pytest.mark.unit
    def test_base_processor_configuration(self):
        """Test BaseProcessor configuration"""
        processor = MockProcessor()

        # Test initial state
        self.assertEqual(processor._config, {})

        # Test configuration
        config = {"test_param": "test_value", "number_param": 42}
        processor.configure(config)
        self.assertEqual(processor._config, config)

        # Test processing
        result = processor.process(self.sample_event)
        self.assertFalse(result.is_empty)
        self.assertEqual(len(result), 1)

        processed_event = result.to_dict()
        self.assertTrue(processed_event["processed_by_mock"])
        self.assertEqual(processed_event["processor_config"], config)

    @pytest.mark.unit
    def test_processor_factory_creation(self):
        """Test ProcessorFactory creation of processors"""
        # Mock processor registry
        with patch("processors.factory.ProcessorRegistry") as mock_registry:
            mock_registry.get.return_value = MockProcessor

            # Test processor creation
            processor = ProcessorFactory.create("mock", test_param="test_value")

            self.assertIsInstance(processor, MockProcessor)
            self.assertEqual(processor._config, {"test_param": "test_value"})
            mock_registry.get.assert_called_once_with("mock")

    @pytest.mark.unit
    def test_process_event_utility(self):
        """Test the process_event utility function"""
        # Test with None processor chain
        result = process_event(self.sample_event, None, {})
        self.assertIsInstance(result, ProcessorResult)
        self.assertEqual(result.to_dict(), self.sample_event)

        # Test with actual processor chain
        mock_proc = MockProcessor()
        mock_proc.configure({"test": "value"})
        chain = ProcessorChain([mock_proc])

        context = {"input_id": "test", "input_type": "s3-sqs"}
        result = process_event(self.sample_event, chain, context)

        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()
        self.assertTrue(processed_event["processed_by_mock"])

    @pytest.mark.unit
    def test_processor_factory_invalid_config(self):
        """Test ProcessorFactory with invalid configurations"""
        # Test missing type field
        with self.assertRaises(ValueError) as context:
            ProcessorFactory.create_chain([{"param": "value"}])

        self.assertIn("'type' field", str(context.exception))

    @pytest.mark.unit
    def test_passthrough_processor(self):
        """Test PassThroughProcessor behavior"""
        processor = PassThroughProcessor()
        processor.configure({})

        result = processor.process(self.sample_event)
        self.assertFalse(result.is_empty)
        self.assertEqual(result.to_dict(), self.sample_event)

    @pytest.mark.unit
    def test_processor_chain_creation(self):
        """Test ProcessorChain creation and basic functionality"""
        # Create a chain with real processors
        proc1 = PassThroughProcessor()
        proc2 = ECSProcessor()

        chain = ProcessorChain([proc1, proc2])
        self.assertEqual(len(chain.processors), 2)

    @pytest.mark.unit
    def test_processor_chain_processing(self):
        """Test ProcessorChain processing events through multiple processors"""
        # Create real processors
        proc1 = PassThroughProcessor()
        proc2 = ECSProcessor()
        proc3 = MockProcessor()
        proc3.configure({"test_config": "chain_test"})

        chain = ProcessorChain([proc1, proc2, proc3])
        context = {"chain_id": "test_chain"}

        # Use IPFIX event for ECS processor
        result = chain.process(self.sample_ipfix_event, context)

        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()

        # Verify processors ran
        self.assertTrue(processed_event["processed_by_mock"])
        self.assertEqual(processed_event["processor_config"], {"test_config": "chain_test"})

    @pytest.mark.unit
    def test_processor_chain_early_termination(self):
        """Test ProcessorChain early termination on empty result"""
        # Create processors where one returns empty result
        proc1 = PassThroughProcessor()

        proc2 = MockEmptyProcessor()  # This should stop the chain

        proc3 = MockProcessor()

        chain = ProcessorChain([proc1, proc2, proc3])

        result = chain.process(self.sample_event)

        # Result should contain the last successful processing (from proc1)
        # since proc2 returned empty and stopped the chain
        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()
        # Verify the event still has the original fields
        self.assertIn("@timestamp", processed_event)
        # MockProcessor should not have run, so no processed_by_mock field
        self.assertNotIn("processed_by_mock", processed_event)

    @pytest.mark.unit
    def test_processor_chain_empty_first_processor(self):
        """Test ProcessorChain when first processor returns empty result"""
        # Create a chain where the first processor returns empty
        proc1 = MockEmptyProcessor()  # This should stop the chain immediately

        proc2 = PassThroughProcessor()

        chain = ProcessorChain([proc1, proc2])

        result = chain.process(self.sample_event)

        # Result should be the original event since first processor returned empty
        # and chain uses the last successful event (which is the original)
        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()
        # Should still contain original event data
        self.assertEqual(processed_event["@timestamp"], self.sample_event["@timestamp"])

    @pytest.mark.unit
    def test_processor_chain_with_filter(self):
        """Test ProcessorChain with filtering processor"""
        # Create a filter that stops processing for specific events
        # Add a field to the event first, then filter on it
        test_event = self.sample_event.copy()
        test_event["should_filter"] = True

        proc1 = PassThroughProcessor()

        proc2 = MockFilterProcessor()
        proc2.configure({"filter_field": "should_filter", "filter_value": True})

        proc3 = MockProcessor()

        chain = ProcessorChain([proc1, proc2, proc3])

        result = chain.process(test_event)

        # Should contain the last successful processing (from proc1)
        # since filter matched and stopped processing
        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()
        self.assertEqual(processed_event["should_filter"], True)
        # MockProcessor should not have run since filter stopped the chain
        self.assertNotIn("processed_by_mock", processed_event)

    @pytest.mark.unit
    def test_processor_chain_with_context(self):
        """Test ProcessorChain passing context through processors"""
        proc1 = PassThroughProcessor()

        proc2 = MockProcessor()
        proc2.configure({"context_test": True})

        chain = ProcessorChain([proc1, proc2])
        context = {"input_id": "test_context", "input_type": "s3-sqs"}

        result = chain.process(self.sample_event, context)

        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()
        self.assertTrue(processed_event["processed_by_mock"])

    @pytest.mark.unit
    def test_processor_chain_empty_list(self):
        """Test ProcessorChain with empty processor list"""
        chain = ProcessorChain([])

        result = chain.process(self.sample_event)

        # Should return the original event unchanged
        self.assertFalse(result.is_empty)
        self.assertEqual(result.to_dict(), self.sample_event)

    @pytest.mark.unit
    def test_processor_factory_create_chain(self):
        """Test ProcessorFactory create_chain method"""
        # Mock the processor registry for this test
        with patch("processors.factory.ProcessorRegistry") as mock_registry:
            # Set up mock registry to return real processors
            def mock_get(processor_type):
                if processor_type == "ipfix_ecs":
                    return ECSProcessor
                elif processor_type == "passthrough":
                    return PassThroughProcessor
                else:
                    raise ValueError(f"Unknown processor type: {processor_type}")

            mock_registry.get.side_effect = mock_get

            # Test creating chain from configurations
            configs = [{"type": "passthrough"}, {"type": "ipfix_ecs"}]

            chain = ProcessorFactory.create_chain(configs)

            self.assertIsInstance(chain, ProcessorChain)
            self.assertEqual(len(chain.processors), 2)
            self.assertIsInstance(chain.processors[0], PassThroughProcessor)
            self.assertIsInstance(chain.processors[1], ECSProcessor)

    @pytest.mark.unit
    def test_processor_factory_create_chain_with_invalid_config(self):
        """Test ProcessorFactory create_chain with invalid configurations"""
        # Test with missing 'type' field
        configs = [{"field_name": "test", "field_value": "value"}]

        with self.assertRaises(ValueError) as context:
            ProcessorFactory.create_chain(configs)

        self.assertIn("'type' field", str(context.exception))

    @pytest.mark.unit
    def test_processor_chain_single_processor(self):
        """Test ProcessorChain with single processor"""
        proc = PassThroughProcessor()

        chain = ProcessorChain([proc])

        result = chain.process(self.sample_event)

        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()
        # Should be unchanged by passthrough processor
        self.assertEqual(processed_event, self.sample_event)

    @pytest.mark.unit
    def test_processor_system_with_ipfix_event(self):
        """Test processor system with IPFIX event data"""
        # Test that the processor system can handle IPFIX events
        proc = ECSProcessor()

        chain = ProcessorChain([proc])
        context = {"input_type": "s3-sqs", "binary_processor_type": "ipfix"}

        result = chain.process(self.sample_ipfix_event, context)

        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()

        # Verify ECS processor converted the message field to JSON string
        self.assertIn("fields", processed_event)
        self.assertIn("message", processed_event["fields"])
        self.assertIsInstance(processed_event["fields"]["message"], str)

    @pytest.mark.unit
    def test_processor_chain_error_handling(self):
        """Test processor chain handles errors gracefully"""

        class ErrorProcessor(BaseProcessor):
            def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
                raise ValueError("Processing error")

        proc1 = PassThroughProcessor()
        error_proc = ErrorProcessor()
        proc3 = PassThroughProcessor()

        chain = ProcessorChain([proc1, error_proc, proc3])

        # The chain should handle the error and stop processing gracefully
        with self.assertRaises(ValueError):
            chain.process(self.sample_event)

    @pytest.mark.unit
    def test_processor_multiple_events_result(self):
        """Test processor that can generate multiple events from one input"""

        class MultiEventProcessor(BaseProcessor):
            def process(self, event: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> ProcessorResult:
                # Create multiple events from one input
                event1 = event.copy()
                event1["event_id"] = 1

                event2 = event.copy()
                event2["event_id"] = 2

                return ProcessorResult([event1, event2])

        multi_proc = MultiEventProcessor()
        chain = ProcessorChain([multi_proc])

        result = chain.process(self.sample_event)

        self.assertFalse(result.is_empty)
        self.assertEqual(len(result), 2)

        result_dict = result.to_dict()
        self.assertIn("0", result_dict)
        self.assertIn("1", result_dict)
        self.assertEqual(result_dict["0"]["event_id"], 1)
        self.assertEqual(result_dict["1"]["event_id"], 2)


class TestProcessorSystemIPFIXIntegration(TestCase):
    """Test processor system integration with IPFIX-specific scenarios"""

    def setUp(self):
        """Set up IPFIX-specific test fixtures"""
        self.ipfix_flow_event = {
            "@timestamp": "2025-01-26T10:30:00Z",
            "fields": {
                "message": {
                    "header": {
                        "version": 10,
                        "length": 844,
                        "export_time": 1719403200,
                        "sequence_number": 1,
                        "observation_domain_id": 0,
                    },
                    "sourceIPv4Address": "192.168.1.100",
                    "destinationIPv4Address": "10.0.0.50",
                    "sourceTransportPort": 443,
                    "destinationTransportPort": 80,
                    "protocolIdentifier": 6,
                    "octetDeltaCount": 1024,
                    "packetDeltaCount": 1,
                    "flowStartSysUpTime": 38074304,
                    "flowEndSysUpTime": 38074304,
                    "@timestamp": "2025-01-26T10:30:00Z",
                }
            },
        }

    @pytest.mark.unit
    def test_ipfix_processor_chain_configuration(self):
        """Test creating processor chains for IPFIX processing"""
        # Mock the registry for IPFIX processor
        with patch("processors.factory.ProcessorRegistry") as mock_registry:

            def mock_get(processor_type):
                if processor_type == "ipfix_ecs":
                    return ECSProcessor
                elif processor_type == "passthrough":
                    return PassThroughProcessor
                else:
                    raise ValueError(f"Unknown processor type: {processor_type}")

            mock_registry.get.side_effect = mock_get

            # Test IPFIX processing chain configuration
            configs = [{"type": "ipfix_ecs"}, {"type": "passthrough"}]

            chain = ProcessorFactory.create_chain(configs)

            self.assertIsInstance(chain, ProcessorChain)
            self.assertEqual(len(chain.processors), 2)
            self.assertIsInstance(chain.processors[0], ECSProcessor)
            self.assertIsInstance(chain.processors[1], PassThroughProcessor)

    @pytest.mark.unit
    def test_ipfix_event_processing_preserves_metadata(self):
        """Test that IPFIX event processing preserves critical metadata"""
        proc = ECSProcessor()

        chain = ProcessorChain([proc])
        context = {"input_type": "s3-sqs", "binary_processor_type": "ipfix", "file_path": "s3://bucket/flows.ipfix.gz"}

        result = chain.process(self.ipfix_flow_event, context)

        self.assertFalse(result.is_empty)
        processed_event = result.to_dict()

        # Verify ECS processor converted the message to JSON string
        self.assertIn("fields", processed_event)
        self.assertIn("message", processed_event["fields"])
        self.assertIsInstance(processed_event["fields"]["message"], str)

        # Parse the converted message to verify IPFIX data was preserved
        from share.json import json_parser

        parsed_message = json_parser(processed_event["fields"]["message"])

        # Verify converted ECS structure
        self.assertIn("source", parsed_message)
        self.assertIn("destination", parsed_message)
        self.assertIn("event", parsed_message)


if __name__ == "__main__":
    pytest.main([__file__])
