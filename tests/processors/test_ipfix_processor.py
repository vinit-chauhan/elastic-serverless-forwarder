
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the Elastic License 2.0;
# you may not use this file except in compliance with the Elastic License 2.0.

"""
Tests for IPFIX to ECS Processor

This module tests the IPFIX to ECS conversion processor including:
- Basic IPFIX field mapping to ECS format
- Network flow enrichment
- Event metadata generation
- Edge cases and error handling
"""

from unittest import TestCase
from typing import Dict, Any

import pytest

from processors.ipfix_ecs import ECSProcessor

# Sample IPFIX event for testing
SAMPLE_IPFIX_EVENT = {
    "header": {
        "version": 10,
        "length": 844,
        "export_time": 1718321356,
        "sequence_number": 0,
        "observation_domain_id": 0,
        "start_offset": 0
    },
    "flowEndSysUpTime": "0024437c",
    "flowStartSysUpTime": "0024437c",
    "octetDeltaCount": "000000ac",
    "packetDeltaCount": "00000001",
    "ipVersion": "04",
    "ingressInterface": "0003",
    "egressInterface": "0004",
    "flowDirection": "00",
    "field_3": "00000000",
    "sourceIPv4Address": "08080808",
    "destinationIPv4Address": "0a640d02",
    "sourceTransportPort": "0035",
    "destinationTransportPort": "8d35",
    "ipClassOfService": "00",
    "tcpControlBits": "00",
    "protocolIdentifier": "11",
    "sourceMacAddress": "56e032c18207",
    "destinationMacAddress": "b4fbe4d0ea7b",
    "vlanId": "0000",
    "mplsLabelStackLength": "00000003",
    "processor": {
        "type": "ipfix",
        "processed_at": 1718321356
    },
    "aws": {
        "s3": {
            "bucket": {
                "name": "ipfix-storage",
                "arn": "arn:aws:s3:::ipfix-storage"
            },
            "object": {
                "key": "10.100.8.1-small.ipfix"
            }
        }
    },
    "cloud": {
        "provider": "aws",
        "region": "us-east-2",
        "account": {
            "id": "816069150995"
        }
    },
    "log": {
        "offset": 5747,
        "file": {
            "path": "https://ipfix-storage.s3.us-east-2.amazonaws.com/10.100.8.1-small.ipfix"
        }
    },
    "meta": {
        "event_time": 0,
        "integration_scope": "generic"
    }
}

# Additional test events for edge cases
MINIMAL_IPFIX_EVENT = {
    "sourceIPv4Address": "c0a80101",  # 192.168.1.1
    "destinationIPv4Address": "08080808",  # 8.8.8.8
    "@timestamp": 1718321356
}

COMPLEX_IPFIX_EVENT = {
    "header": {
        "version": 10,
        "length": 1200,
        "export_time": 1718321356,
        "sequence_number": 100,
        "observation_domain_id": 1
    },
    "sourceIPv4Address": "c0a80164",
    "destinationIPv4Address": "0a000032",
    "sourceTransportPort": "01bb",  # 443
    "destinationTransportPort": "0050",  # 80
    "protocolIdentifier": "06",  # TCP
    "octetDeltaCount": "00002000",  # 8192 bytes
    "packetDeltaCount": "00000010",  # 16 packets
    "flowStartSysUpTime": "024437c0",
    "flowEndSysUpTime": "024437c1",
    "tcpControlBits": "18",  # PSH+ACK
    "ipClassOfService": "00",
    "sourceMacAddress": "001122334455",
    "destinationMacAddress": "aabbccddeeff",
    "@timestamp": 1718321356,
    "processor": {
        "type": "ipfix",
        "processed_at": 1718321356
    }
}


class TestIPFIXECSProcessor(TestCase):
    """Test the IPFIX to ECS processor"""

    def setUp(self):
        """Set up test fixtures"""
        self.processor = ECSProcessor()
        # Wrap the IPFIX data in the expected event structure
        self.sample_event = {
            "fields": {
                "message": SAMPLE_IPFIX_EVENT
            }
        }

    def _parse_ecs_result(self, result):
        """Helper method to parse ECS result from processor"""
        import json
        event_result = result.to_dict()
        return json.loads(event_result['fields']['message'])

    @pytest.mark.unit
    def test_ipfix_ecs_basic_conversion(self):
        """Test basic IPFIX to ECS conversion with standard fields"""
        result = self.processor.process(self.sample_event)

        self.assertFalse(result.is_empty)
        ecs_event = self._parse_ecs_result(result)

        # Verify core ECS fields are present
        self.assertIn('source', ecs_event)
        self.assertIn('destination', ecs_event)
        self.assertIn('network', ecs_event)
        self.assertIn('flow', ecs_event)
        self.assertIn('@timestamp', ecs_event)
        self.assertIn('event', ecs_event)
        self.assertIn('related', ecs_event)
        self.assertIn('netflow', ecs_event)

    @pytest.mark.unit
    def test_ipfix_ecs_source_destination_mapping(self):
        """Test source and destination field mapping"""
        result = self.processor.process(self.sample_event)
        ecs_event = self._parse_ecs_result(result)

        # Verify source fields
        self.assertIn('ip', ecs_event['source'])
        self.assertIn('port', ecs_event['source'])

        # Verify destination fields
        self.assertIn('ip', ecs_event['destination'])
        self.assertIn('port', ecs_event['destination'])

    @pytest.mark.unit
    def test_ipfix_ecs_network_flow_fields(self):
        """Test network and flow field mapping"""
        result = self.processor.process(self.sample_event)
        ecs_event = self._parse_ecs_result(result)

        # Verify network fields
        self.assertIn('transport', ecs_event['network'])
        self.assertIn('bytes', ecs_event['network'])
        self.assertIn('packets', ecs_event['network'])

        # Verify flow fields
        self.assertIn('id', ecs_event['flow'])

    @pytest.mark.unit
    def test_ipfix_ecs_event_metadata(self):
        """Test event metadata generation"""
        result = self.processor.process(self.sample_event)
        ecs_event = self._parse_ecs_result(result)

        # Verify event metadata
        event_metadata = ecs_event['event']
        self.assertEqual(event_metadata['kind'], 'event')
        self.assertIn('network', event_metadata['category'])
        self.assertEqual(event_metadata['type'], ['connection'])

    @pytest.mark.unit
    def test_ipfix_ecs_minimal_event(self):
        """Test ECS conversion with minimal IPFIX event"""
        minimal_event = {
            "fields": {
                "message": MINIMAL_IPFIX_EVENT
            }
        }
        result = self.processor.process(minimal_event)

        self.assertFalse(result.is_empty)
        ecs_event = self._parse_ecs_result(result)

        # Should still have core ECS structure
        self.assertIn('source', ecs_event)
        self.assertIn('destination', ecs_event)
        self.assertIn('event', ecs_event)

    @pytest.mark.unit
    def test_ipfix_ecs_complex_event(self):
        """Test ECS conversion with complex IPFIX event"""
        complex_event = {
            "fields": {
                "message": COMPLEX_IPFIX_EVENT
            }
        }
        result = self.processor.process(complex_event)

        self.assertFalse(result.is_empty)
        ecs_event = self._parse_ecs_result(result)

        # Should have all enriched fields
        self.assertIn('source', ecs_event)
        self.assertIn('destination', ecs_event)
        self.assertIn('network', ecs_event)
        self.assertIn('flow', ecs_event)

        # Should preserve original processor metadata in netflow
        if 'processor' in COMPLEX_IPFIX_EVENT:
            self.assertIn('netflow', ecs_event)
            # The processor info should be in the netflow namespace
            self.assertTrue('processor' in ecs_event.get('netflow', {}))

    @pytest.mark.unit
    def test_ipfix_ecs_processor_configuration(self):
        """Test processor configuration options"""
        # Test with different configurations
        processor = ECSProcessor()
        processor.configure({"mode": "flow", "enrich_geo": False})

        result = processor.process(self.sample_event)
        self.assertFalse(result.is_empty)

    @pytest.mark.unit
    def test_ipfix_ecs_empty_event(self):
        """Test ECS processor with empty event"""
        empty_event = {"fields": {"message": {}}}
        result = self.processor.process(empty_event)

        # Should handle empty events gracefully
        # The exact behavior depends on the processor implementation
        if not result.is_empty:
            ecs_event = self._parse_ecs_result(result)
            self.assertTrue(isinstance(ecs_event, dict))

    @pytest.mark.unit
    def test_ipfix_ecs_related_fields(self):
        """Test that related fields are populated correctly"""
        result = self.processor.process(self.sample_event)
        ecs_event = self._parse_ecs_result(result)

        # Verify related field contains IPs
        if 'related' in ecs_event:
            related = ecs_event['related']
            if 'ip' in related:
                self.assertIsInstance(related['ip'], list)
                self.assertGreater(len(related['ip']), 0)

    @pytest.mark.unit
    def test_ipfix_ecs_preserves_aws_metadata(self):
        """Test that AWS metadata is preserved during conversion"""
        result = self.processor.process(self.sample_event)
        ecs_event = self._parse_ecs_result(result)

        # AWS metadata should be preserved in netflow
        # Check if AWS data was in the original sample and preserved in netflow namespace
        original_ipfix_data = self.sample_event['fields']['message']
        if 'aws' in original_ipfix_data:
            self.assertIn('netflow', ecs_event)
            # AWS metadata should be preserved in the netflow namespace
            self.assertTrue('aws' in ecs_event.get('netflow', {}))
